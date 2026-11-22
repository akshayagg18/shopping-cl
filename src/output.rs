//! Parquet writing: one user_daily file per aggregation state, written in parallel.

use std::path::Path;

use rayon::prelude::*;

use crate::pq::{Column, Kind, write_file};
use crate::{Agg, Lookups, OutInfo, name_bytes};

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
            }
        }
        write_file(&cd.join("part-0.parquet"), &[id, ch, day, s, p, r, a]);
    };

    let user_jobs = || {
        aggs.par_iter().enumerate().for_each(|(i, a)| {
            if a.rows.is_empty() && i > 0 {
                return;
            }
            let n = a.rows.len();
            let bytes: usize = a.rows.iter().map(|r| (r.name & 0xFFFF) as usize + 4).sum();
            let mut ids = Column::new("user_id", Kind::Str, n, bytes);
            let mut day = Column::new("day", Kind::Date, n, n * 4);
            let mut s = Column::new("sessions", Kind::I32, n, n * 4);
            let mut e = Column::new("events", Kind::I32, n, n * 4);
            let mut r = Column::new("revenue_cents", Kind::I64, n, n * 8);
            let mut top = Column::new("top_category", Kind::OptStr, n, n * 12);
            for (k, row) in a.rows.iter().enumerate() {
                // names are scattered (link files, per-thread name buffers): fetch ahead
                if let Some(ahead) = a.rows.get(k + 16) {
                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        std::arch::x86_64::_mm_prefetch(name_bytes(ahead.name).as_ptr() as *const i8, std::arch::x86_64::_MM_HINT_T0)
                    };
                }
                ids.str(name_bytes(row.name));
                day.i32(row.day);
                s.i32(row.sessions as i32);
                e.i32(row.events as i32);
                r.i64(row.revenue);
                top.opt_str(if row.top == 255 { None } else { Some(&cats[row.top as usize]) });
            }
            write_file(&ud.join(format!("part-{i}.parquet")), &[ids, day, s, e, r, top]);
        });
    };
    rayon::join(campaign_job, user_jobs);
}
