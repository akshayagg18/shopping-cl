//! Shopping Sessions: exact, parallel pipeline.
//!
//! Stages:
//!   1. mmap every input; build product/campaign lookups.
//!   2. In parallel: links -> union-find -> person names; events parsed and
//!      scattered by event_id hash.
//!   3. Per event_id bucket: keep the earliest-ingested copy, drop invalid/late,
//!      map user -> person, scatter by person hash. Clicks parsed, validated,
//!      mapped and scattered by person hash.
//!   4. Per person bucket: sort, sessionize, attribute, aggregate.
//!   5. Parquet files written in parallel.

mod alloc;
mod output;
mod parse;
mod profile;

#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use memmap2::Mmap;
use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_64;

use parse::{DAY_MS, split, timestamp};

const P1_BITS: u32 = 9; // event_id buckets
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10; // person buckets
const P2: usize = 1 << P2_BITS;
const CHUNK: usize = 4 << 20;
const SESSION_GAP: i64 = 1_800_000;
const CLICK_REACH: i64 = 7 * DAY_MS;

// ---------------------------------------------------------------- file refs

static EXT_REFS: std::sync::Mutex<Vec<(usize, usize, usize)>> = std::sync::Mutex::new(Vec::new());

static FILES: OnceLock<Vec<Mmap>> = OnceLock::new();

/// file:12 | offset:36 | len:16
#[inline(always)]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    if file < 4095 && off < (1usize << 36) && len <= 0xFFFF {
        ((file as u64) << 52) | ((off as u64) << 16) | len as u64
    } else {
        let mut refs = EXT_REFS.lock().unwrap();
        let i = refs.len();
        assert!(i < (1usize << 52));
        refs.push((file, off, len));
        (4095u64 << 52) | i as u64
    }
}

#[inline(always)]
fn deref(r: u64) -> &'static [u8] {
    let files = FILES.get().unwrap();
    let f = (r >> 52) as usize;
    if f == 4095 {
        let (file, off, len) = EXT_REFS.lock().unwrap()[(r & ((1u64 << 52) - 1)) as usize];
        return &files[file][off..off + len];
    }
    let off = ((r >> 16) & ((1u64 << 36) - 1)) as usize;
    let len = (r & 0xFFFF) as usize;
    &files[f][off..off + len]
}

#[inline(always)]
fn h64(b: &[u8]) -> u64 {
    let h = xxh3_64(b);
    if h == 0 { 1 } else { h }
}

// Lossless 60-bit keys for short ASCII IDs, plus tagged byte-checked hashes
// for every other UTF-8 value. Length/type tags make the encoding injective.
// This is a fast path, never a restriction on the input dialect.
const HASHED_ID: u64 = 1 << 62;
#[inline(always)]
fn id_key(b: &[u8]) -> u64 {
    let n = b.len();
    let short = if n == 9 && matches!(b[0], b'u' | b'U' | b'e' | b'E') {
        let tag = match b[0] {
            b'u' => 9,
            b'U' => 10,
            b'e' => 11,
            _ => 12,
        };
        Some((u64::from_le_bytes(b[1..9].try_into().unwrap()), tag))
    } else if (1..=8).contains(&n) {
        let mut buf = [0u8; 8];
        buf[..n].copy_from_slice(b);
        Some((u64::from_le_bytes(buf), n as u64))
    } else {
        None
    };
    if let Some((word, tag)) = short {
        if word & 0x8080_8080_8080_8080 == 0 {
            #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
            let packed = unsafe { std::arch::x86_64::_pext_u64(word, 0x7F7F_7F7F_7F7F_7F7F) };
            #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
            let packed = (0..8).fold(0u64, |acc, i| acc | (((word >> (8 * i)) & 127) << (7 * i)));
            return (packed << 4) | tag;
        }
    }
    (h64(b) & (HASHED_ID - 1)) | HASHED_ID
}

#[inline(always)]
fn event_hash(stored: u64) -> u64 {
    let k = if stored & LINKED == 0 {
        stored
    } else {
        id_key(deref(event_ref(stored)))
    };
    k.wrapping_mul(0xD6E8_FEB8_6659_FD93).rotate_left(32)
}

// Rare arbitrary UTF-8 IDs retain byte references. The common path carries
// lossless ASCII values, so event records never need a second name reference.
static USER_REFS: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());
#[derive(Default)]
struct UnlinkedNames {
    ids: std::collections::HashMap<&'static [u8], u64>,
    refs: Vec<u64>,
}
static UNLINKED_NAMES: std::sync::LazyLock<std::sync::Mutex<UnlinkedNames>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(UnlinkedNames::default()));
fn store_user(r: u64) -> u64 {
    let mut refs = USER_REFS.lock().unwrap();
    let i = refs.len();
    refs.push(r);
    LINKED | i as u64
}
fn intern_unlinked(r: u64) -> u64 {
    let bytes = deref(r);
    let mut names = UNLINKED_NAMES.lock().unwrap();
    if let Some(&key) = names.ids.get(bytes) {
        return key;
    }
    let key = HASHED_ID | names.refs.len() as u64;
    names.refs.push(r);
    names.ids.insert(bytes, key);
    key
}
impl Persons {
    #[inline(always)]
    fn resolve_stored(&self, user: u64) -> u64 {
        if user & LINKED == 0 {
            return self.map.get(user, &[]).unwrap_or(user);
        }
        let r = USER_REFS.lock().unwrap()[(user & !LINKED) as usize];
        let bytes = deref(r);
        self.map
            .get(id_key(bytes), bytes)
            .unwrap_or_else(|| intern_unlinked(r))
    }
}
#[inline]
fn decode_short<'a>(key: u64, buf: &'a mut [u8; 9]) -> &'a [u8] {
    let tag = (key & 15) as usize;
    let packed = key >> 4;
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let word = unsafe { std::arch::x86_64::_pdep_u64(packed, 0x7F7F_7F7F_7F7F_7F7F) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let word = (0..8).fold(0u64, |acc, i| {
        acc | (((packed >> (7 * i)) & 127) << (8 * i))
    });
    if tag <= 8 {
        buf[..8].copy_from_slice(&word.to_le_bytes());
        &buf[..tag]
    } else {
        buf[0] = match tag {
            9 => b'u',
            10 => b'U',
            11 => b'e',
            12 => b'E',
            _ => unreachable!(),
        };
        buf[1..].copy_from_slice(&word.to_le_bytes());
        &buf[..]
    }
}

// Event storage: lossless short key, or a tagged index into rare byte refs.
// Keep the raw record only when the ID cannot be packed or ingest saturates.
static EVENT_REFS: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());
fn store_event_ref(r: u64) -> u64 {
    let mut refs = EVENT_REFS.lock().unwrap();
    let i = refs.len();
    refs.push(r);
    LINKED | i as u64
}
fn event_ref(key: u64) -> u64 {
    EVENT_REFS.lock().unwrap()[(key & !LINKED) as usize]
}
#[inline(always)]
fn same_event(a: u64, b: u64) -> bool {
    if a == b {
        return true;
    }
    match (a & LINKED != 0, b & LINKED != 0) {
        (false, false) => false,
        (true, true) => deref(event_ref(a)) == deref(event_ref(b)),
        (true, false) => id_key(deref(event_ref(a))) == b,
        (false, true) => a == id_key(deref(event_ref(b))),
    }
}
// Used only for out-of-u32 ingestion dates. Normal generator dates stay compact.
fn full_ingest(e: &Ev1) -> i64 {
    if e.eid & LINKED == 0 {
        return e.ingest as i64;
    }
    let r = event_ref(e.eid);
    let mut f = (r >> 52) as usize;
    let mut off = ((r >> 16) & ((1u64 << 36) - 1)) as usize;
    if f == 4095 {
        let x = EXT_REFS.lock().unwrap()[(r & ((1u64 << 52) - 1)) as usize];
        f = x.0;
        off = x.1;
    }
    let data = file_bytes(f);
    let start = if off > 0 && data[off - 1] == b'"' {
        off - 1
    } else {
        off
    };
    let end = start + memchr(b'\n', &data[start..]).unwrap_or(data.len() - start);
    let line = &data[start..end];
    let fs = split::<7>(line).unwrap();
    parse::ingest_timestamp(&line[fs[3].0 as usize..fs[3].1 as usize]) / 1000
}

// ---------------------------------------------------------------- exact ID map (injective keys or byte-checked hashes)

#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct Slot {
    key: u64,
    val: u64,
}

struct U64Map {
    mask: usize,
    slots: Vec<Slot>,
    refs: Vec<u64>,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map {
            mask: cap - 1,
            slots: vec![Slot::default(); cap],
            refs: vec![0; cap],
        }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = self.slot(k);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }
    /// returns (slot index, existed)
    fn insert_or_get(&mut self, k: u64, v: u64, r: u64) -> (usize, bool) {
        let mut i = self.slot(k);
        loop {
            let kk = self.slots[i].key;
            if kk == k && (k & HASHED_ID == 0 || deref(self.refs[i]) == deref(r)) {
                return (i, true);
            }
            if kk == 0 {
                self.slots[i] = Slot { key: k, val: v };
                if k & HASHED_ID != 0 {
                    self.refs[i] = r;
                }
                return (i, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64, bytes: &[u8]) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let sl = unsafe { self.slots.get_unchecked(i) };
            if sl.key == k && (k & HASHED_ID == 0 || deref(self.refs[i]) == bytes) {
                return Some(sl.val);
            }
            if sl.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- inputs

struct Inputs {
    events: Vec<usize>,
    clicks: Vec<usize>,
    links: Vec<usize>,
    products: usize,
    campaigns: usize,
    window_start: i64,
    window_end: i64,
}

fn csvs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn open_all(data: &Path) -> Inputs {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut push = |p: PathBuf| {
        paths.push(p);
        paths.len() - 1
    };
    let events: Vec<usize> = csvs(&data.join("events"))
        .into_iter()
        .map(&mut push)
        .collect();
    let clicks: Vec<usize> = csvs(&data.join("clicks"))
        .into_iter()
        .map(&mut push)
        .collect();
    let links: Vec<usize> = csvs(&data.join("links"))
        .into_iter()
        .map(&mut push)
        .collect();
    let products = push(data.join("products.csv"));
    let campaigns = push(data.join("campaigns.csv"));
    let populate = std::env::var_os("SS_LAZY").is_none();
    let maps: Vec<Mmap> = paths
        .par_iter()
        .map(|p| {
            let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if len == 0 {
                // mmap of an empty file fails on some systems: map an anonymous empty region.
                memmap2::MmapOptions::new()
                    .len(1)
                    .map_anon()
                    .unwrap()
                    .make_read_only()
                    .unwrap()
            } else {
                unsafe {
                    let mut opts = memmap2::MmapOptions::new();
                    if populate {
                        opts.populate();
                    }
                    opts.map(&f)
                }
                .unwrap()
            }
        })
        .collect();
    // Note: anon placeholder has len 1 with a zero byte; record real lengths below.
    let lens: Vec<usize> = paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0))
        .collect();
    FILES.set(maps).ok();
    LENS.set(lens).ok();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let field = |key: &str| -> i64 {
        let at = manifest.find(key).expect("manifest key") + key.len();
        let rest = &manifest[at..];
        let q1 = rest.find('"').unwrap() + 1; // closing quote of key
        let rest = &rest[q1..];
        let q2 = rest.find('"').unwrap() + 1;
        let rest = &rest[q2..];
        let q3 = rest.find('"').unwrap();
        timestamp(rest[..q3].as_bytes()).expect("manifest timestamp")
    };
    let window_start = field("\"window_start");
    let window_end = field("\"window_end");
    Inputs {
        events,
        clicks,
        links,
        products,
        campaigns,
        window_start,
        window_end,
    }
}

static LENS: OnceLock<Vec<usize>> = OnceLock::new();

#[inline(always)]
fn file_bytes(f: usize) -> &'static [u8] {
    let len = LENS.get().unwrap()[f];
    &FILES.get().unwrap()[f][..len]
}

/// Line-aligned work units: (file, start, end). A line belongs to the unit
/// holding its first byte; the header (first line of each file) is skipped.
fn chunks(files: &[usize]) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for &f in files {
        let n = file_bytes(f).len();
        let mut s = 0;
        while s < n {
            let e = (s + CHUNK).min(n);
            out.push((f, s, e));
            s = e;
        }
    }
    // biggest first helps balancing
    out
}

/// Call `each(line_start, line)` for every line starting in [s, e) except a file's header.
#[inline(always)]
fn for_lines(f: usize, s: usize, e: usize, mut each: impl FnMut(usize, &'static [u8])) {
    let data = file_bytes(f);
    let n = data.len();
    let mut p = s;
    if s == 0 {
        // skip header line
        p = match memchr(b'\n', &data[0..]) {
            Some(i) => i + 1,
            None => n,
        };
    } else if data[s - 1] != b'\n' {
        p = match memchr(b'\n', &data[s..]) {
            Some(i) => s + i + 1,
            None => n,
        };
    }
    while p < e {
        let end = match memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => n,
        };
        each(p, &data[p..end]);
        p = end + 1;
    }
}

/// Iterate records starting in [s, e) (header skipped), split into N fields.
/// `each(line_start, line, fields)`; fields is None for a malformed line.
///
/// Stage 1 builds newline / comma / quote bitmaps for the chunk with SIMD;
/// stage 2 walks lines from the newline bitmap and slices field masks out of
/// the comma and quote bitmaps.
#[inline(always)]
fn for_records<const N: usize>(
    f: usize,
    s: usize,
    e: usize,
    mut each: impl FnMut(usize, &'static [u8], Option<[(u32, u32); N]>),
) {
    // sub-blocks small enough that stage-2 reads hit L2 right after stage 1
    const SUB: usize = 192 << 10;
    let mut a = s;
    while a < e {
        let b = (a + SUB).min(e);
        for_records_block::<N>(f, a, b, &mut each);
        a = b;
    }
}

#[inline(always)]
fn for_records_block<const N: usize>(
    f: usize,
    s: usize,
    e: usize,
    each: &mut impl FnMut(usize, &'static [u8], Option<[(u32, u32); N]>),
) {
    let data = file_bytes(f);
    let n = data.len();
    let mut p = s;
    if s == 0 {
        p = match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => n,
        };
    } else if data[s - 1] != b'\n' {
        p = match memchr(b'\n', &data[s..]) {
            Some(i) => s + i + 1,
            None => n,
        };
    }
    if p >= e {
        return;
    }
    // bitmap covers [base, lim)
    let base = p & !63;
    let lim = (e + 2048).min(n);
    let words = (lim - base).div_ceil(64);
    BITMAPS.with(|bm| {
        let mut bm = bm.borrow_mut();
        let (nlb, cmb, qtb) = bm.prepare(words);
        simd::stage1(&data[base..lim], nlb, cmb, qtb);
        // iterate newline bits starting at p
        let mut w = (p - base) / 64;
        let mut cur = nlb[w] & (u64::MAX << ((p - base) % 64));
        loop {
            // find next newline at or after p
            while cur == 0 {
                w += 1;
                if w >= words {
                    break;
                }
                cur = nlb[w];
            }
            let end = if cur != 0 {
                let pos = base + w * 64 + cur.trailing_zeros() as usize;
                cur &= cur - 1;
                pos
            } else {
                // no newline inside the bitmap range: line runs past it
                match memchr(b'\n', &data[p..]) {
                    Some(i) => p + i,
                    None => n,
                }
            };
            let line = &data[p..end];
            let len = end - p;
            let fields = if len == 0 {
                None
            } else if len < 128 && end <= lim {
                let off = p - base;
                let cm = bits128(cmb, off) & ((1u128 << len) - 1);
                let qm = bits128(qtb, off);
                split_masked::<N>(line, cm, qm)
            } else {
                split::<N>(line)
            };
            each(p, line, fields);
            p = end + 1;
            if p >= e {
                break;
            }
            if end >= lim {
                // finish the rest with the scalar path
                while p < e {
                    let end = match memchr(b'\n', &data[p..]) {
                        Some(i) => p + i,
                        None => n,
                    };
                    let line = &data[p..end];
                    each(p, line, split::<N>(line));
                    p = end + 1;
                }
                break;
            }
        }
    });
}

/// 128 bits of a bitmap starting at bit `off` (words past the end read as 0).
#[inline(always)]
fn bits128(bm: &[u64], off: usize) -> u128 {
    let w = off / 64;
    let sh = off % 64;
    let g = |i: usize| -> u64 { if i < bm.len() { bm[i] } else { 0 } };
    let (a, b, c) = (g(w), g(w + 1), g(w + 2));
    let lo = (a as u128) | ((b as u128) << 64);
    if sh == 0 {
        lo
    } else {
        (lo >> sh) | ((c as u128) << (128 - sh))
    }
}

#[derive(Default)]
struct Bitmaps {
    nl: Vec<u64>,
    cm: Vec<u64>,
    qt: Vec<u64>,
}

impl Bitmaps {
    fn prepare(&mut self, words: usize) -> (&mut [u64], &mut [u64], &mut [u64]) {
        for v in [&mut self.nl, &mut self.cm, &mut self.qt] {
            if v.len() < words {
                v.resize(words, 0);
            }
        }
        (
            &mut self.nl[..words],
            &mut self.cm[..words],
            &mut self.qt[..words],
        )
    }
}

thread_local! {
    static BITMAPS: std::cell::RefCell<Bitmaps> = std::cell::RefCell::new(Bitmaps::default());
}

#[inline(always)]
fn prefix_xor(mut x: u128) -> u128 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x ^= x << 64;
    x
}

/// Fields of a line of `len` (< 128) bytes from its comma and quote masks.
#[inline(always)]
fn split_masked<const N: usize>(line: &[u8], cm: u128, qm: u128) -> Option<[(u32, u32); N]> {
    let len = line.len();
    if cm.count_ones() as usize != N - 1 {
        return None;
    }
    let lm = (1u128 << len) - 1;
    let q = qm & lm;
    let starts = ((cm << 1) | 1) & lm;
    let ends = (cm >> 1) | (1u128 << (len - 1));
    let bad = (q & !(starts | ends)) | ((q.count_ones() & 1) as u128) | (prefix_xor(q) & cm);
    if bad != 0 {
        return None;
    }
    let mut out = [(0u32, 0u32); N];
    let mut c = cm;
    let mut s = 0usize;
    let (qlo, qhi) = (q as u64, (q >> 64) as u64);
    for (k, slot) in out.iter_mut().enumerate() {
        let e = if k + 1 < N {
            let t = c.trailing_zeros() as usize;
            c &= c - 1;
            t
        } else {
            len
        };
        // quoted iff a quote sits at the field start (validated above); branch-free
        let bit = if s < 64 {
            (qlo >> s) & 1
        } else {
            (qhi >> (s & 63)) & 1
        } as usize;
        let bit = bit & ((e > s) as usize);
        *slot = ((s + bit) as u32, (e - bit) as u32);
        s = e + 1;
    }
    Some(out)
}

mod simd {
    /// Newline, comma and quote bitmaps of `d` (64 bytes per word).
    pub fn stage1(d: &[u8], nl: &mut [u64], cm: &mut [u64], qt: &mut [u64]) {
        let full = d.len() / 64;
        for i in 0..full {
            let (a, b, c) = block64(&d[i * 64..i * 64 + 64]);
            nl[i] = a;
            cm[i] = b;
            qt[i] = c;
        }
        if full < nl.len() {
            let mut buf = [0u8; 64];
            let rest = &d[full * 64..];
            buf[..rest.len()].copy_from_slice(rest);
            let (a, b, c) = block64(&buf);
            nl[full] = a;
            cm[full] = b;
            qt[full] = c;
        }
    }

    #[inline(always)]
    fn block64(b: &[u8]) -> (u64, u64, u64) {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            use std::arch::x86_64::*;
            let v0 = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
            let v1 = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
            let m = |c: u8| {
                let k = _mm256_set1_epi8(c as i8);
                (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, k)) as u32 as u64)
                    | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, k)) as u32 as u64) << 32)
            };
            (m(b'\n'), m(b','), m(b'"'))
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        {
            let (mut a, mut c, mut q) = (0u64, 0u64, 0u64);
            for (i, &x) in b[..64].iter().enumerate() {
                a |= ((x == b'\n') as u64) << i;
                c |= ((x == b',') as u64) << i;
                q |= ((x == b'"') as u64) << i;
            }
            (a, c, q)
        }
    }
}

#[inline(always)]
fn memchr(c: u8, hay: &[u8]) -> Option<usize> {
    // SWAR scan, 8 bytes at a time.
    let n = hay.len();
    let mut i = 0;
    let rep = 0x0101_0101_0101_0101u64 * c as u64;
    while i + 8 <= n {
        let w = u64::from_le_bytes(hay[i..i + 8].try_into().unwrap()) ^ rep;
        let t = w.wrapping_sub(0x0101_0101_0101_0101) & !w & 0x8080_8080_8080_8080;
        if t != 0 {
            return Some(i + (t.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }
    while i < n {
        if hay[i] == c {
            return Some(i);
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------- lookups

struct Lookups {
    /// product hash -> category index (sorted order)
    products: U64Map,
    categories: Vec<Vec<u8>>,
    unknown_cat: u8,
    /// campaign hash -> campaign index
    campaigns: U64Map,
    campaign_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

fn lookups(inp: &Inputs) -> Lookups {
    let mut prods: Vec<(u64, &[u8])> = Vec::new();
    let pf = inp.products;
    let n = file_bytes(pf).len();
    for_lines(pf, 0, n, |p, line| {
        if let Some(fs) = split::<2>(line) {
            let a = &line[fs[0].0 as usize..fs[0].1 as usize];
            let b = &line[fs[1].0 as usize..fs[1].1 as usize];
            prods.push((mkref(pf, p + fs[0].0 as usize, a.len()), b));
        }
    });
    let mut cats: Vec<Vec<u8>> = prods.iter().map(|(_, c)| c.to_vec()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() < 255, "too many categories");
    let cat_index = |c: &[u8]| cats.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u64;
    let mut products = U64Map::with_capacity(prods.len());
    for (p, c) in &prods {
        products.insert_or_get(id_key(deref(*p)), cat_index(c), *p);
    }
    let unknown_cat = cat_index(b"unknown") as u8;

    let mut campaigns = U64Map::with_capacity(4096);
    let mut campaign_names = Vec::new();
    let mut channels = Vec::new();
    let cf = inp.campaigns;
    let n = file_bytes(cf).len();
    for_lines(cf, 0, n, |p, line| {
        if let Some(fs) = split::<2>(line) {
            let a = &line[fs[0].0 as usize..fs[0].1 as usize];
            let b = &line[fs[1].0 as usize..fs[1].1 as usize];
            let (_, existed) = campaigns.insert_or_get(
                id_key(a),
                campaign_names.len() as u64,
                mkref(cf, p + fs[0].0 as usize, a.len()),
            );
            if !existed {
                campaign_names.push(a.to_vec());
                channels.push(b.to_vec());
            }
        }
    });
    assert!(campaign_names.len() < 65_535);
    Lookups {
        products,
        categories: cats,
        unknown_cat,
        campaigns,
        campaign_names,
        channels,
    }
}

// ---------------------------------------------------------------- links

const LINKED: u64 = 1 << 63;

/// Linked keys are LINKED | component index. Unlinked keys are reversible
/// short IDs or HASHED_ID | an exact index in UNLINKED_NAMES.
struct Persons {
    /// user hash -> person key, for every linked ID
    map: U64Map,
    /// component index -> ref of its byte-wise smallest ID
    names: Vec<u64>,
}

#[inline(always)]
fn person_bucket(key: u64) -> usize {
    (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - P2_BITS)) as usize
}

fn links(inp: &Inputs) -> Persons {
    let span = profile::Span::new("links", 0);
    let units = chunks(&inp.links);
    let pairs: Vec<Vec<(u64, u64, u64, u64)>> = units
        .par_iter()
        .map(|&(f, s, e)| {
            let mut v = Vec::new();
            for_records::<2>(f, s, e, |p, line, fs| {
                if let Some(fs) = fs {
                    let (a0, a1) = (fs[0].0 as usize, fs[0].1 as usize);
                    let (b0, b1) = (fs[1].0 as usize, fs[1].1 as usize);
                    if a1 > a0 && b1 > b0 {
                        v.push((
                            id_key(&line[a0..a1]),
                            mkref(f, p + a0, a1 - a0),
                            id_key(&line[b0..b1]),
                            mkref(f, p + b0, b1 - b0),
                        ));
                    }
                }
            });
            v
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut index = U64Map::with_capacity(total * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    let mut refs: Vec<u64> = Vec::with_capacity(total);
    let mut hashes: Vec<u64> = Vec::with_capacity(total);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    let mut intern = |h: u64, r: u64, parent: &mut Vec<u32>| -> u32 {
        let id = parent.len() as u64;
        let (slot, existed) = index.insert_or_get(h, id, r);
        if existed {
            index.slots[slot].val as u32
        } else {
            parent.push(id as u32);
            refs.push(r);
            hashes.push(h);
            id as u32
        }
    };
    for v in &pairs {
        for &(ha, ra, hb, rb) in v {
            let a = intern(ha, ra, &mut parent);
            let b = intern(hb, rb, &mut parent);
            let (x, y) = (find(&mut parent, a), find(&mut parent, b));
            if x != y {
                if x < y {
                    parent[y as usize] = x;
                } else {
                    parent[x as usize] = y;
                }
            }
        }
    }
    let n = parent.len();
    // component index per root, smallest name per component
    let mut comp: Vec<u32> = vec![u32::MAX; n];
    let mut names: Vec<u64> = Vec::new();
    let mut map = U64Map::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if comp[r] == u32::MAX {
            comp[r] = names.len() as u32;
            names.push(refs[r]);
        }
        let c = comp[r] as usize;
        if deref(refs[i]) < deref(names[c]) {
            names[c] = refs[i];
        }
        map.insert_or_get(hashes[i], LINKED | c as u64, refs[i]);
    }
    if profile::enabled() {
        eprintln!(
            "PROFILE_LINKS valid_pairs={total} linked_ids={n} components={}",
            names.len()
        );
        span.finish(total, inp.links.iter().map(|&f| file_bytes(f).len()).sum());
    }
    Persons { map, names }
}

// ---------------------------------------------------------------- records

/// A parsed event copy, before dedup. 28 bytes; no redundant probe hash.
/// `user` is an exact short ID or a tagged USER_REFS index, resolved after dedup.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Ev1 {
    eid: u64,
    user: u64,
    ts: u32, // ms since window start; INVALID when the copy fails validation
    ingest: u32,
    pc: u32, // price << 8 | category; price 0 = not a purchase
}

const INVALID: u32 = u32::MAX;

/// A surviving event, keyed by person. 16 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev2 {
    key: u64,
    ts: u32,
    pc: u32,
}

/// A valid click, keyed by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    key: u64,
    cref: u64,
    ts: i64, // ms relative to window start (may be negative)
    camp: u32,
}

/// One parse unit's events, grouped by event_id bucket inside one allocation.
struct EvChunk {
    data: Vec<std::mem::MaybeUninit<Ev1>>,
    cap: usize,
    len: Vec<u32>,
    overflow: Vec<Vec<Ev1>>,
}

impl EvChunk {
    #[inline(always)]
    fn bucket(&self, b: usize) -> (&[Ev1], &[Ev1]) {
        let s = b * self.cap;
        // Only this prefix was written by the parser; slack stays uninitialized.
        let initialized = unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().add(s).cast::<Ev1>(),
                self.len[b] as usize,
            )
        };
        (initialized, &self.overflow[b])
    }
    #[inline(always)]
    fn bucket_len(&self, b: usize) -> usize {
        self.len[b] as usize + self.overflow[b].len()
    }
}

fn parse_events(units: &[(usize, usize, usize)], lk: &Lookups, ws: i64, we: i64) -> Vec<EvChunk> {
    units
        .par_iter()
        .map(|&(f, s, e)| {
            let span = profile::Span::new("parse_events", s);
            // Estimates only: overflow vectors preserve correctness for short
            // or skewed records. Keep each slab within one huge-page mapping.
            let cap =
                ((e - s) / 60 / P1 * 5 / 4 + 16).min((2 << 20) / (P1 * std::mem::size_of::<Ev1>()));
            let mut data: Vec<std::mem::MaybeUninit<Ev1>> = Vec::with_capacity(cap * P1);
            #[allow(clippy::uninit_vec)]
            unsafe {
                data.set_len(cap * P1);
            }
            let mut len = vec![0u32; P1];
            let mut overflow: Vec<Vec<Ev1>> = (0..P1).map(|_| Vec::new()).collect();
            for_records::<7>(f, s, e, |p, line, fs| {
                let Some(fs) = fs else { return };
                let fld =
                    |i: usize| unsafe { line.get_unchecked(fs[i].0 as usize..fs[i].1 as usize) };
                let eid = fld(0);
                if eid.is_empty() {
                    return;
                }
                let ingest_ms = parse::ingest_timestamp(fld(3));
                let eid_key = id_key(eid);
                let eid_hash = eid_key.wrapping_mul(0xD6E8_FEB8_6659_FD93).rotate_left(32);
                let ingest_secs = ingest_ms.div_euclid(1000);
                let stored_eid =
                    if eid_key & HASHED_ID != 0 || !(0..=u32::MAX as i64).contains(&ingest_secs) {
                        store_event_ref(mkref(f, p + fs[0].0 as usize, eid.len()))
                    } else {
                        eid_key
                    };
                let mut ev = Ev1 {
                    eid: stored_eid,
                    user: 0,
                    ts: INVALID,
                    ingest: (ingest_ms.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32,
                    pc: 0,
                };
                'valid: {
                    let user = fld(1);
                    if user.is_empty() {
                        break 'valid;
                    }
                    let Some(ts) = timestamp(fld(2)) else {
                        break 'valid;
                    };
                    if ts < ws || ts >= we || ingest_ms - ts > DAY_MS {
                        break 'valid;
                    }
                    if fld(5).is_empty() {
                        break 'valid;
                    }
                    let ty = fld(4);
                    let pc = if ty == b"view" || ty == b"cart" {
                        0
                    } else if ty == b"purchase" {
                        let Some(pr) = parse::price(fld(6)) else {
                            break 'valid;
                        };
                        let cat = lk
                            .products
                            .get(id_key(fld(5)), fld(5))
                            .map_or(lk.unknown_cat as u64, |c| c)
                            as u32;
                        (pr << 8) | cat
                    } else {
                        break 'valid;
                    };
                    let user_key = id_key(user);
                    ev.user = if user_key & HASHED_ID == 0 {
                        user_key
                    } else {
                        store_user(mkref(f, p + fs[1].0 as usize, user.len()))
                    };
                    ev.ts = (ts - ws) as u32;
                    ev.pc = pc;
                }
                let b = (eid_hash >> (64 - P1_BITS)) as usize;
                let l = unsafe { len.get_unchecked_mut(b) };
                if (*l as usize) < cap {
                    unsafe { data.get_unchecked_mut(b * cap + *l as usize).write(ev) };
                    *l += 1;
                } else {
                    overflow[b].push(ev);
                }
            });
            span.finish(
                len.iter().map(|&v| v as usize).sum::<usize>()
                    + overflow.iter().map(Vec::len).sum::<usize>(),
                e - s,
            );
            EvChunk {
                data,
                cap,
                len,
                overflow,
            }
        })
        .collect()
}

/// Counting-sort records into person buckets: (records, offsets[P2 + 1]).
/// Only the exact-size output is freshly allocated.
fn by_bucket<T: Copy>(
    items: &[T],
    key: impl Fn(&T) -> usize,
    ks: &mut Vec<u16>,
) -> (Vec<T>, Vec<u32>) {
    let mut counts = vec![0u32; P2 + 1];
    ks.clear();
    for it in items {
        let k = key(it);
        counts[k + 1] += 1;
        ks.push(k as u16);
    }
    for i in 0..P2 {
        counts[i + 1] += counts[i];
    }
    let mut pos = counts.clone();
    let mut out: Vec<T> = Vec::with_capacity(items.len());
    #[allow(clippy::uninit_vec)]
    unsafe {
        for (it, &k) in items.iter().zip(ks.iter()) {
            let at = pos.get_unchecked_mut(k as usize);
            out.as_mut_ptr().add(*at as usize).write(*it);
            *at += 1;
        }
        out.set_len(items.len());
    }
    (out, counts)
}

#[derive(Default)]
struct DedupScratch {
    all: Vec<Ev1>,
    table: Vec<u64>,
    keep: Vec<u8>,
    out: Vec<Ev2>,
    ks: Vec<u16>,
}

thread_local! {
    static DEDUP: std::cell::RefCell<DedupScratch> = std::cell::RefCell::new(DedupScratch::default());
}

/// Dedup one event_id bucket, map users to persons, bucket by person.
fn dedup_bucket(parts: &[EvChunk], b: usize, persons: &Persons) -> (Vec<Ev2>, Vec<u32>) {
    let span = profile::Span::new("dedup", b);
    DEDUP.with(|cell| {
        let mut guard = cell.borrow_mut();
        let sc = &mut *guard;
        let n: usize = parts.iter().map(|p| p.bucket_len(b)).sum();
        sc.all.clear();
        for p in parts {
            let (a, o) = p.bucket(b);
            sc.all.extend_from_slice(a);
            sc.all.extend_from_slice(o);
        }
        let all = &sc.all;
        // table entry: full 32-bit probe hash << 32 | index + 1; 0 = empty.
        // Fingerprint matches compare the original ID bytes, including duplicates.
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, 0);
        sc.keep.clear();
        sc.keep.resize(n, 0);
        let (table, keep) = (&mut sc.table, &mut sc.keep);
        for i in 0..n {
            let e = unsafe { all.get_unchecked(i) };
            let h = event_hash(e.eid);
            let fp = h as u32 as u64;
            let mut s = h as usize & mask;
            loop {
                let t = unsafe { *table.get_unchecked(s) };
                if t == 0 {
                    table[s] = (fp << 32) | (i as u64 + 1);
                    keep[i] = 1;
                    break;
                }
                if t >> 32 == fp {
                    let j = (t & 0xFFFF_FFFF) as usize - 1;
                    let o = unsafe { all.get_unchecked(j) };
                    if same_event(o.eid, e.eid) {
                        if e.ingest < o.ingest
                            || (e.ingest == o.ingest
                                && (e.ingest == 0 || e.ingest == u32::MAX)
                                && full_ingest(e) < full_ingest(o))
                        {
                            table[s] = (fp << 32) | (i as u64 + 1);
                            keep[j] = 0;
                            keep[i] = 1;
                        }
                        break;
                    }
                }
                s = (s + 1) & mask;
            }
        }
        const AHEAD: usize = 16;
        sc.out.clear();
        let out = &mut sc.out;
        let mut pending: [u32; AHEAD] = [0; AHEAD];
        let mut np = 0usize;
        let mut head = 0usize;
        let emit = |i: usize, out: &mut Vec<Ev2>| {
            let e = &all[i];
            let key = persons.resolve_stored(e.user);
            out.push(Ev2 {
                key,
                ts: e.ts,
                pc: e.pc,
            });
        };
        for i in 0..n {
            if keep[i] != 0 && all[i].ts != INVALID {
                if all[i].user & LINKED == 0 {
                    persons.map.prefetch(all[i].user);
                }
                if np == AHEAD {
                    // ring: emit the oldest
                    let j = pending[head] as usize;
                    pending[head] = i as u32;
                    head = (head + 1) & (AHEAD - 1);
                    emit(j, out);
                } else {
                    pending[np] = i as u32;
                    np += 1;
                }
            }
        }
        for k in 0..np {
            emit(pending[(head + k) & (AHEAD - 1)] as usize, out);
        }
        let result = by_bucket(&sc.out, |e| person_bucket(e.key), &mut sc.ks);
        span.finish(n, n * std::mem::size_of::<Ev1>());
        result
    })
}

fn parse_clicks(
    units: &[(usize, usize, usize)],
    lk: &Lookups,
    persons: &Persons,
    ws: i64,
    we: i64,
) -> Vec<(Vec<Ck>, Vec<u32>)> {
    let lo = ws - CLICK_REACH;
    units
        .par_iter()
        .map(|&(f, s, e)| {
            let span = profile::Span::new("parse_clicks", s);
            let mut v: Vec<Ck> = Vec::with_capacity((e - s) / 40 + 16);
            for_records::<4>(f, s, e, |p, line, fs| {
                let Some(fs) = fs else { return };
                let fld =
                    |i: usize| unsafe { line.get_unchecked(fs[i].0 as usize..fs[i].1 as usize) };
                let cid = fld(0);
                let user = fld(1);
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let uh = id_key(user);
                persons.map.prefetch(uh);
                let Some(ts) = timestamp(fld(3)) else { return };
                if ts < lo || ts >= we {
                    return;
                }
                let Some(camp) = lk.campaigns.get(id_key(fld(2)), fld(2)) else {
                    return;
                };
                let key = if uh & HASHED_ID == 0 {
                    persons.map.get(uh, &[]).unwrap_or(uh)
                } else {
                    persons.map.get(uh, user).unwrap_or_else(|| {
                        intern_unlinked(mkref(f, p + fs[1].0 as usize, user.len()))
                    })
                };
                v.push(Ck {
                    key,
                    cref: mkref(f, p + fs[0].0 as usize, cid.len()),
                    ts: ts - ws,
                    camp: camp as u32,
                });
            });
            let mut ks = Vec::new();
            let result = by_bucket(&v, |c| person_bucket(c.key), &mut ks);
            span.finish(v.len(), e - s);
            result
        })
        .collect()
}

// ---------------------------------------------------------------- aggregation

pub struct UserRow {
    pub name: u64, // exact person key; decoded/referenced only when writing
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: i64,
    pub top: u8, // 255 = null
}

pub struct Agg {
    pub rows: Vec<UserRow>,
    /// [(camp) * ndays + day] -> sessions, purchasing, revenue, assisted
    pub camp: Vec<[i64; 4]>,
}

struct Ctx<'a> {
    ndays: usize,
    ncamp: usize, // organic = ncamp
    ncat: usize,
    ws: i64,
    ws_day: i64,
    persons: &'a Persons,
}

struct Scratch {
    count: Vec<u32>,  // per campaign count within window
    active: Vec<u32>, // active campaign list
    pos: Vec<u32>,    // position in active list
    cat: Vec<i64>,
    // per-bucket grouping
    table: Vec<(u64, u32)>,
    used: Vec<u32>,
    ev_cnt: Vec<u32>,
    ck_cnt: Vec<u32>,
    ev_local: Vec<u32>,
    ck_local: Vec<u32>,
    evs: Vec<Ev2>,
    cks: Vec<Ck>,
    evs_sorted: Vec<Ev2>,
    cks_sorted: Vec<Ck>,
}

impl Scratch {
    fn new(ncamp: usize) -> Self {
        Scratch {
            count: vec![0; ncamp + 1],
            active: Vec::new(),
            pos: vec![0; ncamp + 1],
            cat: vec![0; 256],
            table: Vec::new(),
            used: Vec::new(),
            ev_cnt: Vec::new(),
            ck_cnt: Vec::new(),
            ev_local: Vec::new(),
            ck_local: Vec::new(),
            evs: Vec::new(),
            cks: Vec::new(),
            evs_sorted: Vec::new(),
            cks_sorted: Vec::new(),
        }
    }
}

/// Group one person bucket's events and clicks by person, then sessionize.
fn process_bucket(ctx: &Ctx, agg: &mut Agg, sc: &mut Scratch) {
    let n = sc.evs.len();
    if n == 0 {
        return;
    }
    let grouping = profile::Span::new("grouping", n);
    // local person ids
    let cap = (n * 2).next_power_of_two().max(64);
    if sc.table.len() < cap {
        sc.table.resize(cap, (0, u32::MAX));
    }
    let mask = cap - 1;
    sc.used.clear();
    sc.ev_cnt.clear();
    sc.ev_local.clear();
    let slot_of = |k: u64| (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & mask;
    for e in &sc.evs {
        let mut s = slot_of(e.key);
        let id = loop {
            let t = unsafe { sc.table.get_unchecked_mut(s) };
            if t.1 == u32::MAX {
                *t = (e.key, sc.ev_cnt.len() as u32);
                sc.used.push(s as u32);
                sc.ev_cnt.push(0);
                break t.1;
            }
            if t.0 == e.key {
                break t.1;
            }
            s = (s + 1) & mask;
        };
        sc.ev_cnt[id as usize] += 1;
        sc.ev_local.push(id);
    }
    let np = sc.ev_cnt.len();
    sc.ck_cnt.clear();
    sc.ck_cnt.resize(np, 0);
    sc.ck_local.clear();
    for c in &sc.cks {
        let mut s = slot_of(c.key);
        let id = loop {
            let t = unsafe { sc.table.get_unchecked(s) };
            if t.1 == u32::MAX {
                break u32::MAX;
            }
            if t.0 == c.key {
                break t.1;
            }
            s = (s + 1) & mask;
        };
        if id != u32::MAX {
            sc.ck_cnt[id as usize] += 1;
        }
        sc.ck_local.push(id);
    }
    for &s in &sc.used {
        sc.table[s as usize] = (0, u32::MAX);
    }
    // prefix sums -> start offsets (ev_cnt / ck_cnt become running positions)
    let mut acc = 0u32;
    for c in sc.ev_cnt.iter_mut() {
        let v = *c;
        *c = acc;
        acc += v;
    }
    let mut cacc = 0u32;
    for c in sc.ck_cnt.iter_mut() {
        let v = *c;
        *c = cacc;
        cacc += v;
    }
    sc.evs_sorted.clear();
    sc.evs_sorted.reserve(n);
    sc.cks_sorted.clear();
    sc.cks_sorted.reserve(cacc as usize);
    let mut ev_pos = sc.ev_cnt.clone();
    for (e, &id) in sc.evs.iter().zip(&sc.ev_local) {
        let at = &mut ev_pos[id as usize];
        unsafe {
            sc.evs_sorted.as_mut_ptr().add(*at as usize).write(*e);
        }
        *at += 1;
    }
    let mut ck_pos = sc.ck_cnt.clone();
    for (c, &id) in sc.cks.iter().zip(&sc.ck_local) {
        if id != u32::MAX {
            let at = &mut ck_pos[id as usize];
            unsafe {
                sc.cks_sorted.as_mut_ptr().add(*at as usize).write(*c);
            }
            *at += 1;
        }
    }
    unsafe {
        sc.evs_sorted.set_len(n);
        sc.cks_sorted.set_len(cacc as usize);
    }
    let evs = std::mem::take(&mut sc.evs_sorted);
    let cks = std::mem::take(&mut sc.cks_sorted);
    let mut evs = evs;
    let mut cks = cks;
    grouping.finish(
        n + sc.cks.len(),
        n * std::mem::size_of::<Ev2>() + sc.cks.len() * std::mem::size_of::<Ck>(),
    );
    for pid in 0..np {
        let (es, ee) = (sc.ev_cnt[pid] as usize, ev_pos[pid] as usize);
        let (cs, ce) = (sc.ck_cnt[pid] as usize, ck_pos[pid] as usize);
        let ev = &mut evs[es..ee];
        let sorting = if ev.len() > 10000 {
            Some(profile::Span::new("heavy_sort", ev.len()))
        } else {
            None
        };
        if ev.len() > 1 {
            ev.sort_unstable_by_key(|e| e.ts);
        }
        let ck = &mut cks[cs..ce];
        if ck.len() > 1 {
            ck.sort_unstable_by(|a, b| {
                a.ts.cmp(&b.ts)
                    .then_with(|| deref(a.cref).cmp(deref(b.cref)))
            });
        }
        if let Some(sorting) = sorting {
            sorting.finish(
                ev.len(),
                ev.len() * std::mem::size_of::<Ev2>() + ck.len() * std::mem::size_of::<Ck>(),
            );
        }
        person_sessions(ctx, ev, ck, agg, sc);
    }
    sc.evs_sorted = evs;
    sc.cks_sorted = cks;
}

fn person_sessions(ctx: &Ctx, evs: &[Ev2], cks: &[Ck], agg: &mut Agg, sc: &mut Scratch) {
    let key = evs[0].key;
    let name = key;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day: i64 = i64::MIN;
    let (mut d_sess, mut d_evs, mut d_rev) = (0u32, 0u32, 0i64);
    let mut d_purch = false;
    let mut k = 0usize;
    while k < evs.len() {
        let start = evs[k].ts as i64;
        let day = (ctx.ws + start).div_euclid(DAY_MS) - ctx.ws_day;
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(ctx, agg, sc, name, cur_day, d_sess, d_evs, d_rev, d_purch);
            }
            cur_day = day;
            d_sess = 0;
            d_evs = 0;
            d_rev = 0;
            d_purch = false;
        }
        let mut last = start;
        let mut n = 0u32;
        let mut rev = 0i64;
        let mut purch = false;
        let mut m = k;
        while m < evs.len() && evs[m].ts as i64 - last <= SESSION_GAP {
            let e = &evs[m];
            last = e.ts as i64;
            n += 1;
            if e.pc >= 256 {
                let price = (e.pc >> 8) as i64;
                rev += price;
                purch = true;
                sc.cat[(e.pc & 0xFF) as usize] += price;
            }
            m += 1;
        }
        k = m;
        // attribution window [start - 7d, start]
        while hi < cks.len() && cks[hi].ts <= start {
            let c = cks[hi].camp as usize;
            if sc.count[c] == 0 {
                sc.pos[c] = sc.active.len() as u32;
                sc.active.push(c as u32);
            }
            sc.count[c] += 1;
            hi += 1;
        }
        while lo < hi && cks[lo].ts < start - CLICK_REACH {
            let c = cks[lo].camp as usize;
            sc.count[c] -= 1;
            if sc.count[c] == 0 {
                let p = sc.pos[c] as usize;
                let lastc = *sc.active.last().unwrap();
                sc.active[p] = lastc;
                sc.pos[lastc as usize] = p as u32;
                sc.active.pop();
            }
            lo += 1;
        }
        let du = day as usize;
        let credit = if lo < hi {
            let t = cks[hi - 1].ts;
            let mut f = hi - 1;
            while f > lo && cks[f - 1].ts == t {
                f -= 1;
            }
            cks[f].camp as usize
        } else {
            ctx.ncamp
        };
        let slot = &mut agg.camp[credit * ctx.ndays + du];
        slot[0] += 1;
        if purch {
            slot[1] += 1;
            slot[2] += rev;
        }
        for &c in &sc.active {
            agg.camp[c as usize * ctx.ndays + du][3] += 1;
        }
        d_sess += 1;
        d_evs += n;
        d_rev += rev;
        d_purch |= purch;
    }
    flush_day(ctx, agg, sc, name, cur_day, d_sess, d_evs, d_rev, d_purch);
    while lo < hi {
        sc.count[cks[lo].camp as usize] = 0;
        lo += 1;
    }
    sc.active.clear();
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn flush_day(
    ctx: &Ctx,
    agg: &mut Agg,
    sc: &mut Scratch,
    name: u64,
    day: i64,
    s: u32,
    e: u32,
    r: i64,
    purch: bool,
) {
    let mut top = 255u8;
    if purch {
        let mut best = 0i64;
        for c in 0..ctx.ncat {
            let v = sc.cat[c];
            if v > best {
                best = v;
                top = c as u8;
            }
            sc.cat[c] = 0;
        }
    }
    agg.rows.push(UserRow {
        name,
        day: (ctx.ws_day + day) as i32,
        sessions: s,
        events: e,
        revenue: r,
        top,
    });
}

fn main() {
    profile::init();
    let t0 = std::time::Instant::now();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads: usize = env("NCPU", "8").parse().unwrap_or(8);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();
    let timing = std::env::var("SS_TIMING").is_ok();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {what}", t0.elapsed().as_secs_f64() * 1e3);
        }
    };

    let span = profile::Span::new("open", 0);
    let inp = open_all(&data);
    span.finish(LENS.get().unwrap().len(), LENS.get().unwrap().iter().sum());
    lap("open");
    let span = profile::Span::new("lookups", 0);
    let lk = lookups(&inp);
    if profile::enabled() {
        let products = lk.products.slots.iter().filter(|s| s.key != 0).count();
        span.finish(
            products + lk.campaign_names.len(),
            file_bytes(inp.products).len() + file_bytes(inp.campaigns).len(),
        );
    }
    let (ws, we) = (inp.window_start, inp.window_end);
    let ev_units = chunks(&inp.events);
    let ck_units = chunks(&inp.clicks);
    let phase = profile::Span::new("links_parse_phase", 0);
    let (persons, ev_parts) = if std::env::var("SS_SEQ").is_ok() {
        let p = links(&inp);
        lap("links");
        let e = parse_events(&ev_units, &lk, ws, we);
        lap("events parsed");
        (p, e)
    } else {
        rayon::join(|| links(&inp), || parse_events(&ev_units, &lk, ws, we))
    };
    let copies = if profile::enabled() {
        ev_parts
            .iter()
            .map(|p| {
                p.len.iter().map(|&n| n as usize).sum::<usize>()
                    + p.overflow.iter().map(Vec::len).sum::<usize>()
            })
            .sum::<usize>()
    } else {
        0
    };
    if profile::enabled() {
        phase.finish(
            copies,
            inp.events
                .iter()
                .chain(&inp.links)
                .map(|&f| file_bytes(f).len())
                .sum(),
        );
    }
    lap("links + events parsed");
    let phase = profile::Span::new("dedup_click_phase", 0);
    let (ev_scattered, ck_parts) = if std::env::var("SS_SEQ").is_ok() {
        let a = (0..P1)
            .into_par_iter()
            .map(|b| dedup_bucket(&ev_parts, b, &persons))
            .collect::<Vec<_>>();
        lap("dedup");
        let b = parse_clicks(&ck_units, &lk, &persons, ws, we);
        lap("clicks");
        (a, b)
    } else {
        rayon::join(
            || {
                (0..P1)
                    .into_par_iter()
                    .map(|b| dedup_bucket(&ev_parts, b, &persons))
                    .collect::<Vec<_>>()
            },
            || parse_clicks(&ck_units, &lk, &persons, ws, we),
        )
    };
    drop(ev_parts);
    let surviving = ev_scattered.iter().map(|(v, _)| v.len()).sum::<usize>();
    let clicks = ck_parts.iter().map(|(v, _)| v.len()).sum::<usize>();
    if profile::enabled() {
        phase.finish(
            copies + clicks,
            copies * std::mem::size_of::<Ev1>()
                + inp
                    .clicks
                    .iter()
                    .map(|&f| file_bytes(f).len())
                    .sum::<usize>(),
        );
    }
    lap("dedup + clicks");

    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncamp = lk.campaign_names.len();
    assert!(we - ws < u32::MAX as i64, "window too long");
    let ctx = Ctx {
        ndays,
        ncamp,
        ncat: lk.categories.len(),
        ws,
        ws_day,
        persons: &persons,
    };
    // A bounded set of long-lived workers owns aggregation and scratch state.
    // Acquire partitions dynamically: retain balancing without Rayon's many
    // fold states (and one Parquet file per state).
    let next = std::sync::atomic::AtomicUsize::new(0);
    let phase = profile::Span::new("session_phase", 0);
    let aggs: Vec<Agg> = (0..threads)
        .into_par_iter()
        .map(|_| {
            let mut agg = Agg {
                rows: Vec::new(),
                camp: vec![[0i64; 4]; (ncamp + 1) * ndays],
            };
            let mut sc = Scratch::new(ncamp);
            loop {
                let b = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if b >= P2 {
                    break;
                }
                let span = profile::Span::new("session_bucket", b);
                let gather = profile::Span::new("session_gather", b);
                sc.evs.clear();
                sc.cks.clear();
                for (v, off) in &ev_scattered {
                    sc.evs
                        .extend_from_slice(&v[off[b] as usize..off[b + 1] as usize]);
                }
                for (v, off) in &ck_parts {
                    sc.cks
                        .extend_from_slice(&v[off[b] as usize..off[b + 1] as usize]);
                }
                gather.finish(
                    sc.evs.len(),
                    sc.evs.len() * std::mem::size_of::<Ev2>()
                        + sc.cks.len() * std::mem::size_of::<Ck>(),
                );
                process_bucket(&ctx, &mut agg, &mut sc);
                span.finish(
                    sc.evs.len(),
                    sc.evs.len() * std::mem::size_of::<Ev2>()
                        + sc.cks.len() * std::mem::size_of::<Ck>(),
                );
            }
            agg
        })
        .collect();
    phase.finish(
        surviving + clicks,
        surviving * std::mem::size_of::<Ev2>() + clicks * std::mem::size_of::<Ck>(),
    );
    lap("sessionized");
    let span = profile::Span::new("output", 0);
    let output_rows = aggs.iter().map(|a| a.rows.len()).sum();
    output::write(&out, aggs, &lk, &ctx_info(&ctx, &lk));
    span.finish(output_rows, 0);
    lap("written");
    profile::summary();
}

pub struct OutInfo<'a> {
    persons: &'a Persons,
    pub ndays: usize,
    pub ncamp: usize,
    pub ws_day: i64,
}

fn ctx_info<'a>(ctx: &Ctx<'a>, _lk: &Lookups) -> OutInfo<'a> {
    OutInfo {
        persons: ctx.persons,
        ndays: ctx.ndays,
        ncamp: ctx.ncamp,
        ws_day: ctx.ws_day,
    }
}

pub fn name_len(key: u64, info: &OutInfo) -> usize {
    if key & LINKED != 0 {
        deref(info.persons.names[(key & !LINKED) as usize]).len()
    } else if key & HASHED_ID != 0 {
        let r = UNLINKED_NAMES.lock().unwrap().refs[(key & !HASHED_ID) as usize];
        deref(r).len()
    } else {
        let tag = (key & 15) as usize;
        if tag <= 8 { tag } else { 9 }
    }
}
pub fn name_bytes<'a>(key: u64, info: &OutInfo, buf: &'a mut [u8; 9]) -> &'a [u8] {
    if key & LINKED != 0 {
        deref(info.persons.names[(key & !LINKED) as usize])
    } else if key & HASHED_ID != 0 {
        let r = UNLINKED_NAMES.lock().unwrap().refs[(key & !HASHED_ID) as usize];
        deref(r)
    } else {
        decode_short(key, buf)
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    #[test]
    fn forced_collisions_and_lossless_refs() {
        let mut bytes = Vec::new();
        let mut field = |v: &[u8]| {
            let r = mkref(0, bytes.len(), v.len());
            bytes.extend_from_slice(v);
            r
        };
        let ea = field(b"ecey3zm4z");
        let eb = field(b"eumrqscuq");
        let ea2 = field(b"ecey3zm4z");
        let ua = field(b"user-a");
        let ub = field(b"user-b");
        assert_ne!(id_key(b"e12345678"), id_key(b"E12345678"));
        assert_ne!(id_key(b"u12345678"), id_key(b"e12345678"));
        assert_ne!(id_key(b"a"), id_key(b"a\0"));
        assert_ne!(id_key(b"u12345678"), id_key(b"U12345678"));
        assert_ne!(id_key(b"12345678"), id_key(b"u12345678"));
        assert_eq!(id_key("é".as_bytes()) & HASHED_ID, HASHED_ID);
        for v in [b"u12345678", b"U12345678", b"e12345678", b"E12345678"] {
            assert_eq!(decode_short(id_key(v), &mut [0u8; 9]), v.as_slice());
        }
        let mut seen = std::collections::HashMap::new();
        let mut rng = 42u64;
        for _ in 0..100000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let n = (rng as usize % 8) + 1;
            let v: Vec<u8> = (0..n).map(|i| ((rng >> (i * 8)) & 127) as u8).collect();
            let key = id_key(&v);
            assert_eq!(key & HASHED_ID, 0);
            assert_eq!(decode_short(key, &mut [0u8; 9]), v.as_slice());
            if let Some(old) = seen.insert(key, v.clone()) {
                assert_eq!(old, v);
            }
        }
        let long = "a".repeat(65534) + "éz";
        let long_ref = field(long.as_bytes());
        let mut mmap = memmap2::MmapOptions::new()
            .len(bytes.len())
            .map_anon()
            .unwrap();
        mmap.copy_from_slice(&bytes);
        FILES.set(vec![mmap.make_read_only().unwrap()]).unwrap();
        LENS.set(vec![bytes.len()]).unwrap();
        assert_eq!(deref(long_ref), long.as_bytes());
        assert_eq!(deref(mkref(0, 0, 9)), b"ecey3zm4z");
        assert_eq!(
            event_hash(id_key(deref(ea))) as u32,
            event_hash(id_key(deref(eb))) as u32
        );
        let mut map = U64Map::with_capacity(2);
        map.insert_or_get(HASHED_ID | 7, 11, ua);
        map.insert_or_get(HASHED_ID | 7, 22, ub);
        assert_eq!(map.get(HASHED_ID | 7, b"user-a"), Some(11));
        assert_eq!(map.get(HASHED_ID | 7, b"user-b"), Some(22));
        assert_eq!(map.get(HASHED_ID | 7, b"user-c"), None);
        let persons = Persons {
            map: U64Map::with_capacity(0),
            names: Vec::new(),
        };
        assert_eq!(std::mem::size_of::<Ev1>(), 28);
        assert_eq!(std::mem::size_of::<Ev2>(), 16);
        assert_eq!(std::mem::size_of::<Ck>(), 32);
        let mut len = vec![0; P1];
        len[0] = 3;
        let ev = |eid, ingest, ts| Ev1 {
            eid: id_key(deref(eid)),
            user: id_key(deref(ua)),
            ts,
            ingest,
            pc: 0,
        };
        let chunk = EvChunk {
            data: vec![ev(ea, 20, 1), ev(eb, 10, 2), ev(ea2, 10, INVALID)]
                .into_iter()
                .map(std::mem::MaybeUninit::new)
                .collect(),
            cap: 3,
            len,
            overflow: (0..P1).map(|_| Vec::new()).collect(),
        };
        let (dedup, _) = dedup_bucket(&[chunk], 0, &persons);
        assert_eq!(dedup.len(), 1); // earliest invalid event-a cannot be rescued
        assert_eq!(dedup[0].ts, 2);
        let ra = store_event_ref(ea);
        let ra2 = store_event_ref(ea2);
        assert!(same_event(ra, ra2));
        assert!(same_event(id_key(deref(ea)), ra));
        assert!(!same_event(id_key(deref(eb)), ra)); // distinct IDs did not merge event-b
        let mut sc = Scratch::new(1);
        sc.evs = vec![
            Ev2 {
                key: intern_unlinked(ua),
                ts: 0,
                pc: 0,
            },
            Ev2 {
                key: intern_unlinked(ub),
                ts: 0,
                pc: 0,
            },
        ];
        sc.cks = vec![Ck {
            key: intern_unlinked(ub),
            cref: ea,
            ts: 0,
            camp: 0,
        }];
        let ctx = Ctx {
            ndays: 1,
            ncamp: 1,
            ncat: 1,
            ws: 0,
            ws_day: 0,
            persons: &persons,
        };
        let mut agg = Agg {
            rows: Vec::new(),
            camp: vec![[0; 4]; 2],
        };
        process_bucket(&ctx, &mut agg, &mut sc);
        assert_eq!(agg.rows.len(), 2); // identical 63-bit person keys remain distinct
        assert_eq!(agg.camp[0][0], 1); // click belongs only to user-b
        assert_eq!(agg.camp[1][0], 1); // user-a remains organic
    }
}
