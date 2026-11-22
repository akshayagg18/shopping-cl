//! Parquet writing: one user_daily file per aggregation state, written in parallel.

use std::path::Path;

use rayon::prelude::*;

use crate::pq::{Column, Kind, write_file};
use crate::{Agg, Lookups, OutInfo, name_bytes};

/// SS_DIGEST=1: print an order-independent digest of both tables (sums of
/// per-row hashes; a user's name is cut at its last '!', so a dataset of K
/// '!'-tagged copies digests to K times the original; campaign values enter
/// linearly for the same reason).
pub static DIGEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn h64(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

fn user_digest(name: &[u8], row: &crate::UserRow, top: Option<&[u8]>) -> u64 {
    let n = name.iter().rposition(|&c| c == b'!').map_or(name, |i| &name[..i]);
    let mut b = Vec::with_capacity(n.len() + 64);
    b.extend_from_slice(n);
    b.push(0);
    b.extend_from_slice(&row.day.to_le_bytes());
    b.extend_from_slice(&row.sessions.to_le_bytes());
    b.extend_from_slice(&row.events.to_le_bytes());
    b.extend_from_slice(&row.revenue.to_le_bytes());
    match top {
        Some(t) => {
            b.push(1);
            b.extend_from_slice(t)
        }
        None => b.push(0),
    }
    h64(&b)
}

pub fn write(out: &Path, aggs: Vec<Agg>, lk: &Lookups, info: &OutInfo) {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    // merge campaign arrays (small) into [camp * ndays + day] order
    let w = info.ncamp + 1;
    let mut camp = vec![[0i64; 4]; w * info.ndays];
    for a in &aggs {
        for d in 0..info.ndays {
            for c in 0..w {
                let t = &mut camp[c * info.ndays + d];
                let cr = a.credit[d * w + c];
                t[0] += (cr & 0xFFFF_FFFF) as i64;
                t[1] += (cr >> 32) as i64;
                t[2] += a.credit_rev[d * w + c];
                t[3] += a.assist[d * w + c] as i64;
            }
        }
    }

    let cats = &lk.categories;
    let campaign_job = || {
        let n = camp.iter().filter(|v| v[0] != 0 || v[3] != 0).count();
        let mut id = Column::new("campaign_id", Kind::Str, n, n * 12);
        let mut ch = Column::new("channel", Kind::Str, n, n * 12);
        let mut day = Column::new("day", Kind::Date, n, n * 4);
        let mut s = Column::new("sessions", Kind::I64, n, n * 8);
        let mut p = Column::new("purchasing_sessions", Kind::I64, n, n * 8);
        let mut r = Column::new("revenue_cents", Kind::I64, n, n * 8);
        let mut a = Column::new("assisted_sessions", Kind::I64, n, n * 8);
        let dig = DIGEST.load(std::sync::atomic::Ordering::Relaxed);
        let mut cd_sum = 0u64;
        for c in 0..=info.ncamp {
            for d in 0..info.ndays {
                let v = camp[c * info.ndays + d];
                if v[0] == 0 && v[3] == 0 {
                    continue;
                }
                if c == info.ncamp {
                    id.str(b"organic");
                    ch.str(b"organic");
                } else {
                    id.str(&lk.campaign_names[c]);
                    ch.str(&lk.channels[c]);
                }
                day.i32((info.ws_day + d as i64) as i32);
                s.i64(v[0]);
                p.i64(v[1]);
                r.i64(v[2]);
                a.i64(v[3]);
                if dig {
                    let name: &[u8] = if c == info.ncamp { b"organic" } else { &lk.campaign_names[c] };
                    let mut k = name.to_vec();
                    k.extend_from_slice(&((info.ws_day + d as i64) as i32).to_le_bytes());
                    let hk = h64(&k);
                    let lin = (v[0] as u64)
                        .wrapping_add((v[1] as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                        .wrapping_add((v[2] as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F))
                        .wrapping_add((v[3] as u64).wrapping_mul(0x1656_67B1_9E37_79F9));
                    cd_sum = cd_sum.wrapping_add(hk.wrapping_mul(lin));
                }
            }
        }
        if dig {
            eprintln!("digest cd {cd_sum:016x} rows {n}");
        }
        write_file(&cd.join("part-0.parquet"), &[id, ch, day, s, p, r, a]);
    };

    let user_jobs = || {
        // equal shares of all rows per file (workers' row counts differ)
        let total: usize = aggs.iter().map(|a| a.rows.len()).sum();
        let nf = rayon::current_num_threads().max(1);
        let dig = DIGEST.load(std::sync::atomic::Ordering::Relaxed);
        let ud_sum = std::sync::atomic::AtomicU64::new(0);
        let mut starts = vec![0usize; aggs.len() + 1];
        for (k, a) in aggs.iter().enumerate() {
            starts[k + 1] = starts[k] + a.rows.len();
        }
        (0..nf).into_par_iter().for_each(|i| {
            let (lo, hi) = (total * i / nf, total * (i + 1) / nf);
            if lo == hi && i > 0 {
                return;
            }
            // the rows [lo, hi) of the concatenation, as slices of the aggs
            let mut parts: Vec<&[crate::UserRow]> = Vec::new();
            for (k, a) in aggs.iter().enumerate() {
                let (s0, s1) = (starts[k].max(lo), starts[k + 1].min(hi));
                if s0 < s1 {
                    parts.push(&a.rows[s0 - starts[k]..s1 - starts[k]]);
                }
            }
            let n = hi - lo;
            let bytes: usize = parts.iter().flat_map(|p| p.iter()).map(|r| (r.name & 0xFFFF) as usize + 4).sum();
            let mut ids = Column::new_dict("user_id", n, bytes);
            let mut day = Column::new("day", Kind::Date, n, n * 4);
            let mut s = Column::new("sessions", Kind::I32, n, n * 4);
            let mut e = Column::new("events", Kind::I32, n, n * 4);
            let mut r = Column::new("revenue_cents", Kind::I64, n, n * 8);
            let mut top = Column::new("top_category", Kind::OptStr, n, n * 12);
            let mut my = 0u64;
            for rows in parts {
                for (k, row) in rows.iter().enumerate() {
                    // names are scattered (link files, per-thread name buffers): fetch ahead
                    if let Some(ahead) = rows.get(k + 16) {
                        #[cfg(target_arch = "x86_64")]
                        unsafe {
                            std::arch::x86_64::_mm_prefetch(name_bytes(ahead.name).as_ptr() as *const i8, std::arch::x86_64::_MM_HINT_T0)
                        };
                    }
                    // a person's rows are adjacent and share its name ref
                    ids.str_run(row.name, || name_bytes(row.name));
                    day.i32(row.day);
                    s.i32(row.sessions as i32);
                    e.i32(row.events as i32);
                    r.i64(row.revenue);
                    top.opt_str(if row.top == 255 { None } else { Some(&cats[row.top as usize]) });
                    if dig {
                        let t: Option<&[u8]> = if row.top == 255 { None } else { Some(&cats[row.top as usize]) };
                        my = my.wrapping_add(user_digest(name_bytes(row.name), row, t));
                    }
                }
            }
            ud_sum.fetch_add(my, std::sync::atomic::Ordering::Relaxed);
            write_file(&ud.join(format!("part-{i}.parquet")), &[ids, day, s, e, r, top]);
        });
        if dig {
            eprintln!("digest ud {:016x} rows {total}", ud_sum.load(std::sync::atomic::Ordering::Relaxed));
        }
    };
    rayon::join(campaign_job, user_jobs);
}
