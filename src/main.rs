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
mod diag;
mod output;
mod parse;

#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, Ordering};

use memmap2::Mmap;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use parse::{DAY_MS, split, timestamp};

const P1_BITS: u32 = 9; // event_id buckets
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10; // person buckets
const P2: usize = 1 << P2_BITS;
const CHUNK: usize = 4 << 20;
const SESSION_GAP: i64 = 1_800_000;
const CLICK_REACH: i64 = 7 * DAY_MS;

// ---------------------------------------------------------------- file refs

/// Input mappings of the current run (leaked so refs stay 'static; one run per process,
/// or a few in diagnostic mode).
static FILES: AtomicPtr<Vec<Mmap>> = AtomicPtr::new(std::ptr::null_mut());
static LENS: AtomicPtr<Vec<usize>> = AtomicPtr::new(std::ptr::null_mut());

/// file:12 | offset:36 | len:16
#[inline(always)]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    ((file as u64) << 52) | ((off as u64) << 16) | (len.min(0xFFFF) as u64)
}

#[inline(always)]
fn deref(r: u64) -> &'static [u8] {
    let files = unsafe { &*FILES.load(Ordering::Relaxed) };
    let f = (r >> 52) as usize;
    let off = ((r >> 16) & ((1u64 << 36) - 1)) as usize;
    let len = (r & 0xFFFF) as usize;
    &files[f][off..off + len]
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

/// The two little-endian words of a <= 16 byte string, zero padded.
#[inline(always)]
fn words16(b: &[u8]) -> (u64, u64) {
    let n = b.len();
    debug_assert!(n <= 16);
    let ld8 = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    let ld4 = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64;
    if n >= 8 {
        // overlapping loads; keep only bytes 8..n of the second word
        let y = if n == 8 { 0 } else { ld8(n - 8) >> (8 * (16 - n)) };
        (ld8(0), y)
    } else if n >= 4 {
        (ld4(0) | (ld4(n - 4) << (8 * (n - 4))), 0)
    } else {
        let mut x = 0u64;
        for (i, &c) in b.iter().enumerate() {
            x |= (c as u64) << (8 * i);
        }
        (x, 0)
    }
}

/// 64-bit ID hash, never 0. Short IDs (the common case) take two 128-bit
/// multiplies; longer ones use xxh3.
#[inline(always)]
fn h64(b: &[u8]) -> u64 {
    let h = if b.len() <= 16 {
        let (x, y) = words16(b);
        let l = b.len() as u64;
        mum(x ^ 0xa076_1d64_78bd_642f, y ^ 0xe703_7ed1_a0b4_28db ^ (l << 56))
            ^ mum(y ^ 0x8ebc_6af0_9c88_c6e3, x ^ 0x5899_65cc_7537_4cc3 ^ l).rotate_left(32)
    } else {
        xxh3_64(b)
    };
    if h == 0 { 1 } else { h }
}

/// (hash, independent 32-bit check) of an event_id.
#[inline(always)]
fn h64_chk(b: &[u8]) -> (u64, u32) {
    if b.len() <= 16 {
        let (x, y) = words16(b);
        let l = b.len() as u64;
        let h = mum(x ^ 0xa076_1d64_78bd_642f, y ^ 0xe703_7ed1_a0b4_28db ^ (l << 56))
            ^ mum(y ^ 0x8ebc_6af0_9c88_c6e3, x ^ 0x5899_65cc_7537_4cc3 ^ l).rotate_left(32);
        let c = mum(x ^ 0x1d8e_4e27_c47d_124f, y ^ 0x9e37_79b9_7f4a_7c15 ^ l);
        (if h == 0 { 1 } else { h }, (c >> 32) as u32)
    } else {
        (h64(b), xxh3_64_with_seed(b, 0x51_7C_C1_B7) as u32)
    }
}

// ---------------------------------------------------------------- hash map u64 -> u64 (keys already hashed, never 0)

#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct Slot {
    key: u64,
    val: u64,
}

struct U64Map {
    mask: usize,
    slots: Vec<Slot>,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { mask: cap - 1, slots: vec![Slot::default(); cap] }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & self.mask
    }
    /// returns (slot index, existed)
    fn insert_or_get(&mut self, k: u64, v: u64) -> (usize, bool) {
        let mut i = self.slot(k);
        loop {
            let kk = self.slots[i].key;
            if kk == k {
                return (i, true);
            }
            if kk == 0 {
                self.slots[i] = Slot { key: k, val: v };
                return (i, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let sl = unsafe { self.slots.get_unchecked(i) };
            if sl.key == k {
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
    let events: Vec<usize> = csvs(&data.join("events")).into_iter().map(&mut push).collect();
    let clicks: Vec<usize> = csvs(&data.join("clicks")).into_iter().map(&mut push).collect();
    let links: Vec<usize> = csvs(&data.join("links")).into_iter().map(&mut push).collect();
    let products = push(data.join("products.csv"));
    let campaigns = push(data.join("campaigns.csv"));
    let maps: Vec<Mmap> = paths
        .par_iter()
        .map(|p| {
            let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if len == 0 {
                // mmap of an empty file fails on some systems: map an anonymous empty region.
                memmap2::MmapOptions::new().len(1).map_anon().unwrap().make_read_only().unwrap()
            } else {
                // no MAP_POPULATE: page-table work is spread over the workers
                unsafe { memmap2::MmapOptions::new().map(&f) }.unwrap()
            }
        })
        .collect();
    // Note: anon placeholder has len 1 with a zero byte; record real lengths below.
    let lens: Vec<usize> = paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0))
        .collect();
    FILES.store(Box::into_raw(Box::new(maps)), Ordering::Release);
    LENS.store(Box::into_raw(Box::new(lens)), Ordering::Release);

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
    Inputs { events, clicks, links, products, campaigns, window_start, window_end }
}


#[inline(always)]
fn file_bytes(f: usize) -> &'static [u8] {
    let lens: &'static Vec<usize> = unsafe { &*LENS.load(Ordering::Relaxed) };
    let files: &'static Vec<Mmap> = unsafe { &*FILES.load(Ordering::Relaxed) };
    &files[f][..lens[f]]
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
        (&mut self.nl[..words], &mut self.cm[..words], &mut self.qt[..words])
    }
}

thread_local! {
    static BITMAPS: std::cell::RefCell<Bitmaps> = std::cell::RefCell::new(Bitmaps::default());
}

#[inline(always)]
fn prefix_xor64(x: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq"))]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(v) as u64
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "pclmulqdq")))]
    {
        let mut x = x;
        x ^= x << 1;
        x ^= x << 2;
        x ^= x << 4;
        x ^= x << 8;
        x ^= x << 16;
        x ^= x << 32;
        x
    }
}

#[inline(always)]
fn prefix_xor(x: u128) -> u128 {
    let lo = prefix_xor64(x as u64);
    let hi = prefix_xor64((x >> 64) as u64) ^ (0u64.wrapping_sub(lo >> 63));
    (lo as u128) | ((hi as u128) << 64)
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
        let bit = if s < 64 { (qlo >> s) & 1 } else { (qhi >> (s & 63)) & 1 } as usize;
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
    let mut prods: Vec<(&[u8], &[u8])> = Vec::new();
    let pf = inp.products;
    let n = file_bytes(pf).len();
    for_lines(pf, 0, n, |_, line| {
        if let Some(fs) = split::<2>(line) {
            let a = &line[fs[0].0 as usize..fs[0].1 as usize];
            let b = &line[fs[1].0 as usize..fs[1].1 as usize];
            prods.push((a, b));
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
        products.insert_or_get(h64(p), cat_index(c));
    }
    let unknown_cat = cat_index(b"unknown") as u8;

    let mut campaigns = U64Map::with_capacity(4096);
    let mut campaign_names = Vec::new();
    let mut channels = Vec::new();
    let cf = inp.campaigns;
    let n = file_bytes(cf).len();
    for_lines(cf, 0, n, |_, line| {
        if let Some(fs) = split::<2>(line) {
            let a = &line[fs[0].0 as usize..fs[0].1 as usize];
            let b = &line[fs[1].0 as usize..fs[1].1 as usize];
            let (_, existed) = campaigns.insert_or_get(h64(a), campaign_names.len() as u64);
            if !existed {
                campaign_names.push(a.to_vec());
                channels.push(b.to_vec());
            }
        }
    });
    assert!(campaign_names.len() < 65_535);
    Lookups { products, categories: cats, unknown_cat, campaigns, campaign_names, channels }
}

// ---------------------------------------------------------------- links

const LINKED: u64 = 1 << 63;

/// Person keys: a linked ID maps to LINKED | component index; any other ID's
/// key is its own hash with the top bit cleared.
struct Persons {
    /// user hash -> person key, for every linked ID
    map: PersonMap,
    /// component index -> ref of its byte-wise smallest ID
    names: Vec<u64>,
}

/// Open addressing keyed by the hash's top bits, built from keys sorted by hash
/// (so construction is one streaming pass), with a 1-bit-per-slot filter that
/// answers most "not linked" lookups from a few MiB.
struct PersonMap {
    shift: u32,
    slots: Vec<Slot>,
    filter: Vec<u64>,
    fmask: u64,
}

impl PersonMap {
    /// `sorted` must be sorted by key and contain distinct, non-zero keys.
    fn build(sorted: &[(u64, u64)]) -> Self {
        let n = sorted.len().max(1);
        let bits = ((n + n / 3).next_power_of_two().max(64)).trailing_zeros();
        let cap = 1usize << bits;
        let mut slots = vec![Slot::default(); cap + 64];
        let mut next = 0usize;
        for &(k, v) in sorted {
            let home = (k >> (64 - bits)) as usize;
            let at = home.max(next);
            if at >= slots.len() {
                slots.resize(at + 64, Slot::default());
            }
            slots[at] = Slot { key: k, val: v };
            next = at + 1;
        }
        let fbits = ((n * 24).next_power_of_two().max(1 << 12)) as u64;
        let mut filter = vec![0u64; (fbits / 64) as usize];
        let fmask = fbits - 1;
        for &(k, _) in sorted {
            let i = k & fmask;
            filter[(i >> 6) as usize] |= 1 << (i & 63);
        }
        PersonMap { shift: 64 - bits, slots, filter, fmask }
    }
    #[inline(always)]
    fn maybe(&self, k: u64) -> bool {
        let i = k & self.fmask;
        unsafe { (*self.filter.get_unchecked((i >> 6) as usize) >> (i & 63)) & 1 != 0 }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        if self.maybe(k) {
            let i = (k >> self.shift) as usize;
            #[cfg(target_arch = "x86_64")]
            unsafe {
                use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
                _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
            }
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        if !self.maybe(k) {
            return None;
        }
        let mut i = (k >> self.shift) as usize;
        loop {
            let sl = unsafe { self.slots.get_unchecked(i) };
            if sl.key == k {
                return Some(sl.val);
            }
            // keys sit in sorted order: an empty slot or a larger key ends the run
            if sl.key == 0 || sl.key > k {
                return None;
            }
            i += 1;
        }
    }
}

#[inline(always)]
fn unlinked_key(user_hash: u64) -> u64 {
    user_hash & !LINKED
}

#[inline(always)]
fn person_bucket(key: u64) -> usize {
    (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - P2_BITS)) as usize
}

/// Big-endian first 16 bytes and length: compares like the full byte string
/// whenever the prefixes differ or both strings are at most 16 bytes.
#[inline(always)]
fn name_key(b: &[u8]) -> (u128, u32) {
    let mut buf = [0u8; 16];
    let n = b.len().min(16);
    buf[..n].copy_from_slice(&b[..n]);
    (u128::from_be_bytes(buf), b.len() as u32)
}

#[inline(always)]
fn name_less(a: (u128, u32, u64), b: (u128, u32, u64)) -> bool {
    if a.0 != b.0 {
        return a.0 < b.0;
    }
    if a.1 <= 16 && b.1 <= 16 {
        return a.1 < b.1;
    }
    deref(a.2) < deref(b.2)
}

fn links(inp: &Inputs) -> Persons {
    let units = chunks(&inp.links);
    // endpoints of every valid link line, in line order: (hash, ref)
    let ends: Vec<Vec<(u64, u64)>> = units
        .par_iter()
        .map(|&(f, s, e)| {
            let mut v = Vec::new();
            for_records::<2>(f, s, e, |p, line, fs| {
                if let Some(fs) = fs {
                    let (a0, a1) = (fs[0].0 as usize, fs[0].1 as usize);
                    let (b0, b1) = (fs[1].0 as usize, fs[1].1 as usize);
                    if a1 > a0 && b1 > b0 {
                        v.push((h64(&line[a0..a1]), mkref(f, p + a0, a1 - a0)));
                        v.push((h64(&line[b0..b1]), mkref(f, p + b0, b1 - b0)));
                    }
                }
            });
            v
        })
        .collect();
    let ends: Vec<(u64, u64)> = ends.concat();
    // dense ids: distinct hashes in sorted order
    let mut uniq: Vec<(u64, u64)> = ends.clone();
    uniq.par_sort_unstable_by_key(|x| x.0);
    uniq.dedup_by_key(|x| x.0);
    let n = uniq.len();
    let id_of = |h: u64| uniq.binary_search_by_key(&h, |x| x.0).unwrap() as u32;
    let edges: Vec<(u32, u32)> = ends.par_chunks(2).map(|c| (id_of(c[0].0), id_of(c[1].0))).collect();
    drop(ends);
    // union-find over the dense ids (small, cache resident)
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    for &(a, b) in &edges {
        let (x, y) = (find(&mut parent, a), find(&mut parent, b));
        if x != y {
            if x < y {
                parent[y as usize] = x;
            } else {
                parent[x as usize] = y;
            }
        }
    }
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        parent[i] = r;
    }
    // smallest name per component, from cached prefixes
    let keys: Vec<(u128, u32, u64)> = uniq
        .par_iter()
        .map(|&(_, r)| {
            let (k, l) = name_key(deref(r));
            (k, l, r)
        })
        .collect();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = parent[i] as usize;
        if name_less(keys[i], keys[best[r] as usize]) {
            best[r] = i as u32;
        }
    }
    let mut comp: Vec<u32> = vec![u32::MAX; n];
    let mut names: Vec<u64> = Vec::new();
    let mut entries: Vec<(u64, u64)> = Vec::with_capacity(n);
    for i in 0..n {
        let r = parent[i] as usize;
        if comp[r] == u32::MAX {
            comp[r] = names.len() as u32;
            names.push(keys[best[r] as usize].2);
        }
        entries.push((uniq[i].0, LINKED | comp[r] as u64));
    }
    // `uniq` is sorted by hash, so `entries` is too
    Persons { map: PersonMap::build(&entries), names }
}

// ---------------------------------------------------------------- records

/// A parsed event copy, before dedup. 40 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev1 {
    eid: u64,
    user: u64,
    uref: u64,
    ts: u32, // ms since window start; INVALID when the copy fails validation
    ingest: u32,
    chk: u32,
    pc: u32, // price << 8 | category; price 0 = not a purchase
}

const INVALID: u32 = u32::MAX;

/// A surviving event, keyed by person. 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev2 {
    key: u64,
    uref: u64,
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
    data: Vec<Ev1>,
    cap: usize,
    len: Vec<u32>,
    overflow: Vec<Vec<Ev1>>,
}

impl EvChunk {
    #[inline(always)]
    fn bucket(&self, b: usize) -> (&[Ev1], &[Ev1]) {
        let s = b * self.cap;
        (&self.data[s..s + self.len[b] as usize], &self.overflow[b])
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
            // events lines are >= ~60 bytes; per-bucket room with slack
            let cap = (e - s) / 64 / P1 * 9 / 8 + 24;
            let mut data: Vec<Ev1> = Vec::with_capacity(cap * P1);
            #[allow(clippy::uninit_vec)]
            unsafe {
                data.set_len(cap * P1);
            }
            let mut len = vec![0u32; P1];
            let mut overflow: Vec<Vec<Ev1>> = (0..P1).map(|_| Vec::new()).collect();
            for_records::<7>(f, s, e, |p, line, fs| {
                let Some(fs) = fs else { return };
                let fld = |i: usize| unsafe { line.get_unchecked(fs[i].0 as usize..fs[i].1 as usize) };
                let eid = fld(0);
                if eid.is_empty() {
                    return;
                }
                let ingest_ms = timestamp(fld(3)).unwrap_or(i64::MAX / 2);
                let (eid_h, eid_c) = h64_chk(eid);
                let mut ev = Ev1 {
                    eid: eid_h,
                    user: 0,
                    uref: 0,
                    ts: INVALID,
                    ingest: (ingest_ms.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32,
                    chk: eid_c,
                    pc: 0,
                };
                'valid: {
                    let user = fld(1);
                    if user.is_empty() {
                        break 'valid;
                    }
                    let Some(ts) = timestamp(fld(2)) else { break 'valid };
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
                        let Some(pr) = parse::price(fld(6)) else { break 'valid };
                        let cat = lk.products.get(h64(fld(5))).map_or(lk.unknown_cat as u64, |c| c) as u32;
                        (pr << 8) | cat
                    } else {
                        break 'valid;
                    };
                    ev.user = h64(user);
                    ev.uref = mkref(f, p + fs[1].0 as usize, user.len());
                    ev.ts = (ts - ws) as u32;
                    ev.pc = pc;
                }
                let b = (ev.eid >> (64 - P1_BITS)) as usize;
                let l = unsafe { len.get_unchecked_mut(b) };
                if (*l as usize) < cap {
                    unsafe { *data.get_unchecked_mut(b * cap + *l as usize) = ev };
                    *l += 1;
                } else {
                    overflow[b].push(ev);
                }
            });
            EvChunk { data, cap, len, overflow }
        })
        .collect()
}

/// Counting-sort records into person buckets: (records, offsets[P2 + 1]).
/// Only the exact-size output is freshly allocated.
fn by_bucket<T: Copy>(items: &[T], key: impl Fn(&T) -> usize, ks: &mut Vec<u16>) -> (Vec<T>, Vec<u32>) {
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
        out.set_len(items.len());
        for (it, &k) in items.iter().zip(ks.iter()) {
            let at = pos.get_unchecked_mut(k as usize);
            *out.get_unchecked_mut(*at as usize) = *it;
            *at += 1;
        }
    }
    (out, counts)
}

#[derive(Default)]
struct DedupScratch {
    table: Vec<u64>,
    keep: Vec<u8>,
    out: Vec<Ev2>,
    ks: Vec<u16>,
    bases: Vec<usize>,
}

thread_local! {
    static DEDUP: std::cell::RefCell<DedupScratch> = std::cell::RefCell::new(DedupScratch::default());
}

/// Dedup one event_id bucket in place over the parse chunks, map users to
/// persons, bucket by person.
fn dedup_bucket(parts: &[EvChunk], b: usize, persons: &Persons) -> (Vec<Ev2>, Vec<u32>) {
    DEDUP.with(|cell| {
        let mut guard = cell.borrow_mut();
        let sc = &mut *guard;
        // global index g over (chunk main slice, chunk overflow) in order
        sc.bases.clear();
        let mut n = 0usize;
        for p in parts {
            sc.bases.push(n);
            n += p.bucket_len(b);
        }
        let bases = &sc.bases;
        let at = |g: usize| -> &Ev1 {
            let c = bases.partition_point(|&x| x <= g) - 1;
            let (m, o) = parts[c].bucket(b);
            let i = g - bases[c];
            if i < m.len() { &m[i] } else { &o[i - m.len()] }
        };
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, 0);
        sc.keep.clear();
        sc.keep.resize(n, 0);
        let (table, keep) = (&mut sc.table, &mut sc.keep);
        let mut g = 0usize;
        for p in parts {
            let (m, o) = p.bucket(b);
            for e in m.iter().chain(o.iter()) {
                let fp = e.eid >> 32;
                let mut s = (e.eid as usize) & mask;
                loop {
                    let t = unsafe { *table.get_unchecked(s) };
                    if t == 0 {
                        table[s] = (fp << 32) | (g as u64 + 1);
                        keep[g] = 1;
                        break;
                    }
                    if t >> 32 == fp {
                        let j = (t & 0xFFFF_FFFF) as usize - 1;
                        let old = at(j);
                        if old.eid == e.eid && old.chk == e.chk {
                            if e.ingest < old.ingest {
                                table[s] = (fp << 32) | (g as u64 + 1);
                                keep[j] = 0;
                                keep[g] = 1;
                            }
                            break;
                        }
                    }
                    s = (s + 1) & mask;
                }
                g += 1;
            }
        }
        const AHEAD: usize = 16;
        sc.out.clear();
        let out = &mut sc.out;
        let mut ring: [*const Ev1; AHEAD] = [std::ptr::null(); AHEAD];
        let mut head = 0usize;
        let mut filled = 0usize;
        let emit = |e: &Ev1, out: &mut Vec<Ev2>| {
            let key = persons.map.get(e.user).unwrap_or_else(|| unlinked_key(e.user));
            out.push(Ev2 { key, uref: e.uref, ts: e.ts, pc: e.pc });
        };
        let mut g = 0usize;
        for p in parts {
            let (m, o) = p.bucket(b);
            for e in m.iter().chain(o.iter()) {
                if keep[g] != 0 && e.ts != INVALID {
                    persons.map.prefetch(e.user);
                    if filled == AHEAD {
                        emit(unsafe { &*ring[head] }, out);
                    } else {
                        filled += 1;
                    }
                    ring[head] = e as *const Ev1;
                    head = (head + 1) % AHEAD;
                }
                g += 1;
            }
        }
        // drain in insertion order
        let start = if filled == AHEAD { head } else { 0 };
        for k in 0..filled {
            emit(unsafe { &*ring[(start + k) % AHEAD] }, out);
        }
        by_bucket(&sc.out, |e| person_bucket(e.key), &mut sc.ks)
    })
}

fn parse_clicks(units: &[(usize, usize, usize)], lk: &Lookups, persons: &Persons, ws: i64, we: i64) -> Vec<(Vec<Ck>, Vec<u32>)> {
    let lo = ws - CLICK_REACH;
    units
        .par_iter()
        .map(|&(f, s, e)| {
            // key holds the user hash until the batched person lookup below
            let mut v: Vec<Ck> = Vec::with_capacity((e - s) / 40 + 16);
            for_records::<4>(f, s, e, |p, line, fs| {
                let Some(fs) = fs else { return };
                let fld = |i: usize| unsafe { line.get_unchecked(fs[i].0 as usize..fs[i].1 as usize) };
                let cid = fld(0);
                let user = fld(1);
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let Some(ts) = timestamp(fld(3)) else { return };
                if ts < lo || ts >= we {
                    return;
                }
                let Some(camp) = lk.campaigns.get(h64(fld(2))) else { return };
                v.push(Ck { key: h64(user), cref: mkref(f, p + fs[0].0 as usize, cid.len()), ts: ts - ws, camp: camp as u32 });
            });
            const AHEAD: usize = 16;
            let n = v.len();
            for i in 0..n.min(AHEAD) {
                persons.map.prefetch(v[i].key);
            }
            for i in 0..n {
                if i + AHEAD < n {
                    persons.map.prefetch(v[i + AHEAD].key);
                }
                let uh = v[i].key;
                v[i].key = persons.map.get(uh).unwrap_or_else(|| unlinked_key(uh));
            }
            let mut ks = Vec::new();
            by_bucket(&v, |c| person_bucket(c.key), &mut ks)
        })
        .collect()
}

// ---------------------------------------------------------------- aggregation

pub struct UserRow {
    pub name: u64, // ref
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: i64,
    pub top: u8, // 255 = null
}

pub struct Agg {
    pub rows: Vec<UserRow>,
    /// [day * (ncamp + 1) + camp] -> sessions, purchasing, revenue (credited)
    pub credit: Vec<[i64; 3]>,
    /// [day * (ncamp + 1) + camp] -> assisted sessions
    pub assist: Vec<u32>,
}

struct Ctx<'a> {
    ndays: usize,
    ncamp: usize, // organic = ncamp
    ncat: usize,
    ws: i64,
    ws_day: i64,
    persons: &'a Persons,
}

struct Scratch<'s> {
    ev_src: Vec<&'s [Ev2]>,
    ck_src: Vec<&'s [Ck]>,
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
    evs_sorted: Vec<Ev2>,
    cks_sorted: Vec<Ck>,
}

impl Scratch<'_> {
    fn new(ncamp: usize) -> Self {
        Scratch {
            ev_src: Vec::new(),
            ck_src: Vec::new(),
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
            evs_sorted: Vec::new(),
            cks_sorted: Vec::new(),
        }
    }
}

/// Group one person bucket's events and clicks by person, then sessionize.
fn process_bucket(ctx: &Ctx, agg: &mut Agg, sc: &mut Scratch) {
    let n: usize = sc.ev_src.iter().map(|s| s.len()).sum();
    if n == 0 {
        return;
    }
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
    for e in sc.ev_src.iter().flat_map(|s| s.iter()) {
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
    for c in sc.ck_src.iter().flat_map(|s| s.iter()) {
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
    unsafe {
        sc.evs_sorted.set_len(n);
        sc.cks_sorted.set_len(cacc as usize);
    }
    let mut ev_pos = sc.ev_cnt.clone();
    for (e, &id) in sc.ev_src.iter().flat_map(|s| s.iter()).zip(&sc.ev_local) {
        let at = &mut ev_pos[id as usize];
        sc.evs_sorted[*at as usize] = *e;
        *at += 1;
    }
    let mut ck_pos = sc.ck_cnt.clone();
    for (c, &id) in sc.ck_src.iter().flat_map(|s| s.iter()).zip(&sc.ck_local) {
        if id != u32::MAX {
            let at = &mut ck_pos[id as usize];
            sc.cks_sorted[*at as usize] = *c;
            *at += 1;
        }
    }
    let evs = std::mem::take(&mut sc.evs_sorted);
    let cks = std::mem::take(&mut sc.cks_sorted);
    let mut evs = evs;
    let mut cks = cks;
    for pid in 0..np {
        let (es, ee) = (sc.ev_cnt[pid] as usize, ev_pos[pid] as usize);
        let (cs, ce) = (sc.ck_cnt[pid] as usize, ck_pos[pid] as usize);
        let ev = &mut evs[es..ee];
        if ev.len() > 1 {
            ev.sort_unstable_by_key(|e| e.ts);
        }
        let ck = &mut cks[cs..ce];
        if ck.len() > 1 {
            ck.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| deref(a.cref).cmp(deref(b.cref))));
        }
        person_sessions(ctx, ev, ck, agg, sc);
    }
    sc.evs_sorted = evs;
    sc.cks_sorted = cks;
}

fn person_sessions(ctx: &Ctx, evs: &[Ev2], cks: &[Ck], agg: &mut Agg, sc: &mut Scratch) {
    let key = evs[0].key;
    let name = if key & LINKED != 0 { ctx.persons.names[(key & !LINKED) as usize] } else { evs[0].uref };
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
        let row = du * (ctx.ncamp + 1);
        let slot = &mut agg.credit[row + credit];
        slot[0] += 1;
        if purch {
            slot[1] += 1;
            slot[2] += rev;
        }
        let arow = &mut agg.assist[row..row + ctx.ncamp + 1];
        for &c in &sc.active {
            arow[c as usize] += 1;
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
fn flush_day(ctx: &Ctx, agg: &mut Agg, sc: &mut Scratch, name: u64, day: i64, s: u32, e: u32, r: i64, purch: bool) {
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
    agg.rows.push(UserRow { name, day: (ctx.ws_day + day) as i32, sessions: s, events: e, revenue: r, top });
}

fn main() {
    if std::env::var("SS_NOHUGE").is_ok() {
        alloc::HUGE_ON.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads: usize = env("NCPU", "8").parse().unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().unwrap();
    if std::env::var("SS_DIAG").is_ok() {
        diag::main(&data, &out, threads);
        std::process::exit(1);
    }
    let laps = run(&data, &out, true);
    if std::env::var("SS_TIMING").is_ok() {
        for (what, ms) in laps {
            eprintln!("{ms:>8.1} ms  {what}");
        }
    }
    // skip tearing down gigabytes of buffers: the kernel reclaims them at exit
    std::process::exit(0);
}

/// One full pipeline run. Returns cumulative stage times in ms.
pub fn run(data: &Path, out: &Path, leak: bool) -> Vec<(&'static str, f64)> {
    let t0 = std::time::Instant::now();
    let mut laps: Vec<(&'static str, f64)> = Vec::new();
    let mut lap = |what: &'static str| laps.push((what, t0.elapsed().as_secs_f64() * 1e3));

    let inp = open_all(data);
    lap("open");
    let lk = lookups(&inp);
    let (ws, we) = (inp.window_start, inp.window_end);
    let ev_units = chunks(&inp.events);
    let ck_units = chunks(&inp.clicks);
    let seq = std::env::var("SS_SEQ").is_ok();
    let (persons, ev_parts) = if seq {
        let p = links(&inp);
        lap("links");
        let e = parse_events(&ev_units, &lk, ws, we);
        lap("events parsed");
        (p, e)
    } else {
        rayon::join(|| links(&inp), || parse_events(&ev_units, &lk, ws, we))
    };
    lap("links + events parsed");
    let (ev_scattered, ck_parts) = if seq {
        let a = (0..P1).into_par_iter().map(|b| dedup_bucket(&ev_parts, b, &persons)).collect::<Vec<_>>();
        lap("dedup");
        let b = parse_clicks(&ck_units, &lk, &persons, ws, we);
        lap("clicks");
        (a, b)
    } else {
        rayon::join(
            || (0..P1).into_par_iter().map(|b| dedup_bucket(&ev_parts, b, &persons)).collect::<Vec<_>>(),
            || parse_clicks(&ck_units, &lk, &persons, ws, we),
        )
    };
    lap("dedup + clicks");

    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncamp = lk.campaign_names.len();
    assert!(we - ws < u32::MAX as i64, "window too long");
    let ctx = Ctx { ndays, ncamp, ncat: lk.categories.len(), ws, ws_day, persons: &persons };
    // exactly one accumulator per worker thread (rayon's fold would create one
    // per split: hundreds of 0.8 MB counter arrays and output files at 8 threads)
    let nthreads = rayon::current_num_threads();
    let states: Vec<std::sync::Mutex<(Agg, Scratch)>> = (0..nthreads)
        .map(|_| {
            std::sync::Mutex::new((
                Agg { rows: Vec::new(), credit: vec![[0i64; 3]; (ncamp + 1) * ndays], assist: vec![0u32; (ncamp + 1) * ndays] },
                Scratch::new(ncamp),
            ))
        })
        .collect();
    (0..P2).into_par_iter().for_each(|b| {
        let i = rayon::current_thread_index().unwrap_or(0) % nthreads;
        let mut guard = states[i].lock().unwrap();
        let (agg, sc) = &mut *guard;
        sc.ev_src.clear();
        sc.ck_src.clear();
        for (v, off) in &ev_scattered {
            sc.ev_src.push(&v[off[b] as usize..off[b + 1] as usize]);
        }
        for (v, off) in &ck_parts {
            sc.ck_src.push(&v[off[b] as usize..off[b + 1] as usize]);
        }
        process_bucket(&ctx, agg, sc);
    });
    let aggs: Vec<Agg> = states.into_iter().map(|m| m.into_inner().unwrap().0).collect();
    lap("sessionized");
    output::write(out, aggs, &lk, &ctx_info(&ctx, &lk));
    lap("written");
    if leak {
        // the process is about to exit: skip unmapping gigabytes of buffers
        std::mem::forget(ev_parts);
        std::mem::forget(ev_scattered);
        std::mem::forget(ck_parts);
        std::mem::forget(persons);
    }
    laps
}

pub struct OutInfo {
    pub ndays: usize,
    pub ncamp: usize,
    pub ws_day: i64,
}

fn ctx_info(ctx: &Ctx, _lk: &Lookups) -> OutInfo {
    OutInfo { ndays: ctx.ndays, ncamp: ctx.ncamp, ws_day: ctx.ws_day }
}

pub fn name_bytes(r: u64) -> &'static [u8] {
    deref(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_xor_matches() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..100_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let v = ((x as u128) << 64) | (x.rotate_left(17) as u128);
            let mut r = v;
            for k in [1, 2, 4, 8, 16, 32, 64] {
                r ^= r << k;
            }
            assert_eq!(prefix_xor(v), r);
        }
    }
}

#[cfg(test)]
mod tok_tests {
    use super::*;

    fn install(bytes: &[u8]) {
        let mut m = memmap2::MmapMut::map_anon(bytes.len().max(1)).unwrap();
        m[..bytes.len()].copy_from_slice(bytes);
        let maps = vec![m.make_read_only().unwrap()];
        FILES.store(Box::into_raw(Box::new(maps)), Ordering::Release);
        LENS.store(Box::into_raw(Box::new(vec![bytes.len()])), Ordering::Release);
    }

    fn naive<const N: usize>(d: &[u8]) -> Vec<(usize, Option<Vec<(u32, u32)>>)> {
        let mut out = Vec::new();
        let mut p = match d.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => return out,
        };
        while p < d.len() {
            let end = d[p..].iter().position(|&c| c == b'\n').map_or(d.len(), |i| p + i);
            out.push((p, split::<N>(&d[p..end]).map(|f| f.to_vec())));
            p = end + 1;
        }
        out
    }

    #[test]
    fn tokenizer_matches_scalar() {
        let mut x: u64 = 0x1234_5678;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alpha = b"ab,,,\"\"\n\r xy0123456789";
        for round in 0..3040 {
            let big = round >= 3000;
            let len = if big { 300_000 + (rnd() % 300_000) as usize } else { (rnd() % 3000) as usize + 1 };
            let mut d: Vec<u8> = b"h1,h2,h3,h4\n".to_vec();
            for _ in 0..len {
                // mostly well-formed-ish lines
                let r = rnd() % 100;
                let mut c = if r < 70 { b"abcd"[(rnd() % 4) as usize] } else { alpha[(rnd() as usize) % alpha.len()] };
                if big && c == b'\n' && rnd() % 64 != 0 {
                    c = b'z'; // long lines, some past the 2 KiB look-ahead
                }
                d.push(c);
            }
            if round % 3 == 0 {
                d.push(b'\n');
            }
            install(&d);
            let want = naive::<4>(&d);
            // random partition into units
            let n = d.len();
            let mut cuts: Vec<usize> = (0..(rnd() % 6)).map(|_| (rnd() as usize) % n).collect();
            cuts.push(0);
            cuts.push(n);
            cuts.sort();
            cuts.dedup();
            let mut got = Vec::new();
            for w in cuts.windows(2) {
                for_records::<4>(0, w[0], w[1], |p, _line, fs| got.push((p, fs.map(|f| f.to_vec()))));
            }
            assert_eq!(got, want, "round {round}: {:?}", String::from_utf8_lossy(&d));
        }
    }
}

#[cfg(test)]
mod hash_tests {
    use super::*;
    #[test]
    fn words16_matches_copy() {
        let src: Vec<u8> = (0u8..40).map(|i| i.wrapping_mul(37).wrapping_add(1)).collect();
        for a in 0..20 {
            for n in 0..=16 {
                let b = &src[a..a + n];
                let mut buf = [0u8; 16];
                buf[..n].copy_from_slice(b);
                let want = (u64::from_le_bytes(buf[..8].try_into().unwrap()), u64::from_le_bytes(buf[8..].try_into().unwrap()));
                assert_eq!(words16(b), want, "a={a} n={n}");
            }
        }
    }
    #[test]
    fn no_collisions_on_generator_like_ids() {
        let mut seen = std::collections::HashSet::new();
        let mut seen_full = std::collections::HashSet::new();
        let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
        for prefix in [b'u', b'e', b'k'] {
            for i in 0..2_000_000u64 {
                let mut v = (i.wrapping_mul(0x9E37_79B9_7F4B) ^ 0x5DEECE66D) & ((1 << 40) - 1);
                let mut id = vec![prefix];
                let mut tmp = Vec::new();
                loop {
                    tmp.push(digits[(v % 36) as usize]);
                    v /= 36;
                    if v == 0 {
                        break;
                    }
                }
                tmp.reverse();
                id.extend(tmp);
                assert!(seen.insert(h64(&id)), "64-bit collision at {i}");
                assert!(seen_full.insert(h64_chk(&id)));
            }
        }
        // short and empty-ish edge cases are all distinct
        let edge: Vec<&[u8]> = vec![b"", b"\0", b"\0\0", b"a", b"a\0", b"0123456789abcdef", b"0123456789abcdeF"];
        let hs: std::collections::HashSet<u64> = edge.iter().map(|e| h64(e)).collect();
        assert_eq!(hs.len(), edge.len());
    }
}
