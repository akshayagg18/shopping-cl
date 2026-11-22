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
mod flow;
mod pq;
mod scan;
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

use parse::{DAY_MS, Fields, split, timestamp};

const P2_BITS: u32 = 10; // person buckets
#[allow(dead_code)]
const P2: usize = 1 << P2_BITS;

/// Target events per person partition and per dedup partition.
const P2_PER: usize = 16 << 10;
const PD_PER: usize = 16 << 10;

/// Person partitions of this run (bits), set once before any record is routed.
pub static P2B: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(P2_BITS);

#[inline(always)]
pub fn p2_bits() -> u32 {
    P2B.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn p2() -> usize {
    1 << p2_bits()
}
const CHUNK: usize = 4 << 20;
const SESSION_GAP: i64 = 1_800_000;
const CLICK_REACH: i64 = 7 * DAY_MS;

// ---------------------------------------------------------------- file refs

/// Input mappings of the current run (leaked so refs stay 'static; one run per process,
/// or a few in diagnostic mode).
static FILES: AtomicPtr<Vec<Mmap>> = AtomicPtr::new(std::ptr::null_mut());
static SCALAR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LENS: AtomicPtr<Vec<usize>> = AtomicPtr::new(std::ptr::null_mut());
/// Input paths, index-aligned with FILES (files are opened per work unit for
/// pread: keeping hundreds open would grow the fd table, and every growth in a
/// multithreaded process waits for an RCU grace period, tens of ms).
static PATHS: AtomicPtr<Vec<PathBuf>> = AtomicPtr::new(std::ptr::null_mut());
/// Bulk parsing reads through pread into small buffers instead of the mapping.
pub static READ: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn open_input(f: usize) -> File {
    let paths: &'static Vec<PathBuf> = unsafe { &*PATHS.load(Ordering::Acquire) };
    File::open(&paths[f]).unwrap_or_else(|e| panic!("open {}: {e}", paths[f].display()))
}

/// Read bytes [lo, hi) of `file` into `buf[..hi - lo]`. The buffer only ever
/// grows (zeroed once), so a refill costs no memset.
pub fn read_range(file: &File, lo: usize, hi: usize, buf: &mut Vec<u8>) {
    use std::os::unix::fs::FileExt;
    let n = hi - lo;
    if buf.len() < n {
        buf.resize(n, 0);
    }
    let buf = &mut buf[..n];
    let mut got = 0;
    while got < n {
        match file.read_at(&mut buf[got..], (lo + got) as u64) {
            Ok(0) => panic!("short read"),
            Ok(k) => got += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read: {e}"),
        }
    }
}

/// file:12 | offset:36 | len:16
#[inline(always)]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    ((file as u64) << 52) | ((off as u64) << 16) | (len.min(0xFFFF) as u64)
}

/// Per-thread name buffers of the current run, as files numbered after the inputs.
static NAMES: AtomicPtr<Vec<&'static [u8]>> = AtomicPtr::new(std::ptr::null_mut());

#[inline(always)]
fn deref(r: u64) -> &'static [u8] {
    let files = unsafe { &*FILES.load(Ordering::Relaxed) };
    let f = (r >> 52) as usize;
    let off = ((r >> 16) & ((1u64 << 36) - 1)) as usize;
    let len = (r & 0xFFFF) as usize;
    if f < files.len() {
        &files[f][off..off + len]
    } else {
        let names = unsafe { &*NAMES.load(Ordering::Relaxed) };
        &names[f - files.len()][off..off + len]
    }
}

#[allow(dead_code)]
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

/// Two AES rounds over a <= 16 byte ID, zero padded, with its length in the
/// first round key. Each round is a bijection of the 128-bit block, so two
/// distinct IDs of one length never collide in the full output, and the
/// rounds diffuse every input byte into every output byte.
#[cfg(all(target_arch = "x86_64", target_feature = "aes"))]
#[inline(always)]
fn aes16(b: &[u8]) -> (u64, u64) {
    use std::arch::x86_64::*;
    let (x, y) = words16(b);
    unsafe {
        let v = _mm_set_epi64x(y as i64, x as i64);
        let k0 = _mm_set_epi64x(0x243F_6A88_85A3_08D3u64 as i64, (0x1319_8A2E_0370_7344u64 ^ b.len() as u64) as i64);
        let k1 = _mm_set_epi64x(0xA409_3822_299F_31D0u64 as i64, 0x082E_FA98_EC4E_6C89u64 as i64);
        let k2 = _mm_set_epi64x(0x4528_21E6_38D0_1377u64 as i64, 0xBE54_66CF_34E9_0C6Cu64 as i64);
        let r = _mm_aesenc_si128(_mm_aesenc_si128(_mm_xor_si128(v, k0), k1), k2);
        (_mm_cvtsi128_si64(r) as u64, _mm_extract_epi64(r, 1) as u64)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "aes")))]
#[inline(always)]
fn aes16(b: &[u8]) -> (u64, u64) {
    let (x, y) = words16(b);
    let l = b.len() as u64;
    let h = mum(x ^ 0xa076_1d64_78bd_642f, y ^ 0xe703_7ed1_a0b4_28db ^ (l << 56))
        ^ mum(y ^ 0x8ebc_6af0_9c88_c6e3, x ^ 0x5899_65cc_7537_4cc3 ^ l).rotate_left(32);
    (h, mum(x ^ 0x1d8e_4e27_c47d_124f, y ^ 0x9e37_79b9_7f4a_7c15 ^ l))
}

/// 64-bit ID hash, never 0. Short IDs (the common case) take two AES rounds;
/// longer ones use xxh3.
#[inline(always)]
fn h64(b: &[u8]) -> u64 {
    let h = if b.len() <= 16 { aes16(b).0 } else { xxh3_64(b) };
    if h == 0 { 1 } else { h }
}

/// (hash, independent 32-bit check) of an event_id.
#[inline(always)]
fn h64_chk(b: &[u8]) -> (u64, u32) {
    if b.len() <= 16 {
        let (h, c) = aes16(b);
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
    PATHS.store(Box::into_raw(Box::new(paths.clone())), Ordering::Release);
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
    chunks_of(files, CHUNK)
}

fn chunks_of(files: &[usize], size: usize) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for &f in files {
        let n = file_bytes(f).len();
        let mut s = 0;
        while s < n {
            let e = (s + size).min(n);
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
    mut each: impl FnMut(usize, &'static [u8], Option<Fields<N>>),
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
    each: &mut impl FnMut(usize, &'static [u8], Option<Fields<N>>),
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
            } else if len <= 120 && end <= lim {
                let off = p - base;
                let cm = bits128(cmb, off) & ((1u128 << len) - 1);
                let qm = bits128(qtb, off);
                split_masked::<N>(line, cm, qm)
            } else {
                SCALAR.fetch_add(1, Ordering::Relaxed);
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

/// 128 bits of a bitmap starting at bit `off`. The bitmap carries at least 3
/// zero words of padding, so the unaligned 16-byte load below stays in bounds;
/// bits 121..127 may be missing for odd offsets, so callers use it for lines of
/// at most 120 bytes.
#[inline(always)]
fn bits128(bm: &[u64], off: usize) -> u128 {
    debug_assert!(off / 8 + 16 <= bm.len() * 8);
    let v = unsafe { std::ptr::read_unaligned((bm.as_ptr() as *const u8).add(off / 8) as *const u128) };
    v >> (off % 8)
}

#[derive(Default)]
struct Bitmaps {
    nl: Vec<u64>,
    cm: Vec<u64>,
    qt: Vec<u64>,
}

impl Bitmaps {
    /// Bitmaps of `words` words plus 3 zeroed words of padding (for `bits128`).
    fn prepare(&mut self, words: usize) -> (&mut [u64], &mut [u64], &mut [u64]) {
        for v in [&mut self.nl, &mut self.cm, &mut self.qt] {
            if v.len() < words + 3 {
                v.resize(words + 3, 0);
            }
            v[words..words + 3].fill(0);
        }
        (&mut self.nl[..words + 3], &mut self.cm[..words + 3], &mut self.qt[..words + 3])
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
fn split_masked<const N: usize>(line: &[u8], cm: u128, qm: u128) -> Option<Fields<N>> {
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
    // one bit per field: is it quoted? (quote at its start; validated above)
    let qs = q & starts;
    let (slo, shi) = (starts as u64, (starts >> 64) as u64);
    let flags = pext64(qs as u64, slo) | (pext64((qs >> 64) as u64, shi) << slo.count_ones());
    let mut out = Fields { ends: [0u32; N], q: flags as u32 };
    let mut c = cm;
    for k in 0..N - 1 {
        out.ends[k] = c.trailing_zeros();
        c &= c - 1;
    }
    out.ends[N - 1] = len as u32;
    Some(out)
}

#[inline(always)]
fn pext64(x: u64, m: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(x, m)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let (mut r, mut m, mut i) = (0u64, m, 0);
        while m != 0 {
            let t = m.trailing_zeros();
            r |= ((x >> t) & 1) << i;
            i += 1;
            m &= m - 1;
        }
        r
    }
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
            let (a0, a1) = fs.get(0);
            let (b0, b1) = fs.get(1);
            let a = &line[a0..a1];
            let b = &line[b0..b1];
            prods.push((a, b));
        }
    });
    // distinct categories first (a handful), then sort those
    let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    seen.insert(b"unknown");
    for (_, c) in &prods {
        seen.insert(c);
    }
    let mut cats: Vec<Vec<u8>> = seen.into_iter().map(|c| c.to_vec()).collect();
    cats.sort();
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
            let (a0, a1) = fs.get(0);
            let (b0, b1) = fs.get(1);
            let a = &line[a0..a1];
            let b = &line[b0..b1];
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

/// Open addressing keyed by the hash's top bits, keys in sorted order, so a
/// probe stops at an empty slot or a larger key. The table is cut into 256
/// ranges by the top 8 bits, each followed by MARGIN spill slots, so the ranges
/// are built in parallel.
struct PersonMap {
    shift: u32,
    sub: u32,
    slots: Vec<Slot>,
}

const PM_PARTS_BITS: u32 = 8;
const PM_PARTS: usize = 1 << PM_PARTS_BITS;
const PM_MARGIN: usize = 128;

impl PersonMap {
    #[inline(always)]
    fn index(&self, k: u64) -> usize {
        let h = (k >> self.shift) as usize;
        h + (h >> self.sub) * PM_MARGIN
    }

    /// `sorted` must be sorted by key and contain distinct, non-zero keys.
    fn build(sorted: &[(u64, u64)]) -> Self {
        let n = sorted.len().max(1);
        let load_x4: usize = std::env::var("SS_PMLOAD").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
        let bits = ((n * 4 / 3 * 4 / load_x4.max(1)).next_power_of_two().max(1 << 14)).trailing_zeros();
        let sub = bits - PM_PARTS_BITS;
        let range = (1usize << sub) + PM_MARGIN;
        let mut map = PersonMap { shift: 64 - bits, sub, slots: Vec::new() };
        let mut slots: Vec<Slot> = vec![Slot::default(); PM_PARTS * range];
        // partition starts in `sorted`
        let mut starts = vec![0usize; PM_PARTS + 1];
        for p in 0..PM_PARTS {
            starts[p + 1] = sorted.partition_point(|&(k, _)| ((k >> (64 - PM_PARTS_BITS)) as usize) <= p);
        }
        let ok = std::sync::atomic::AtomicBool::new(true);
        let base = slots.as_mut_ptr() as usize;
        (0..PM_PARTS).into_par_iter().for_each(|p| {
            let lim = (p + 1) * range;
            let mut next = p * range;
            for &(k, v) in &sorted[starts[p]..starts[p + 1]] {
                let at = map.index(k).max(next);
                if at >= lim {
                    ok.store(false, Ordering::Relaxed);
                    return;
                }
                // each partition writes only its own range
                unsafe { *(base as *mut Slot).add(at) = Slot { key: k, val: v } };
                next = at + 1;
            }
        });
        if !ok.load(Ordering::Relaxed) {
            // a range overflowed its margin (never seen in practice): one serial pass
            // into a single sorted table with the ranges' gaps kept as slack
            slots.iter_mut().for_each(|s| *s = Slot::default());
            let mut next = 0usize;
            for &(k, v) in sorted {
                let at = map.index(k).max(next);
                if at >= slots.len() {
                    slots.resize(at + PM_MARGIN, Slot::default());
                }
                slots[at] = Slot { key: k, val: v };
                next = at + 1;
            }
        }
        slots.resize(slots.len() + PM_MARGIN, Slot::default());
        map.slots = slots;
        map
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = self.index(k);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }
    /// Lookup; the caller prefetches, so no filter in front (its own load would miss).
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.index(k);
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
    (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - p2_bits())) as usize
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
    use std::sync::atomic::AtomicU32;
    let t0 = std::time::Instant::now();
    let tl = std::env::var_os("SS_LT").is_some();
    let lt = |w: &str| if tl { eprintln!("  links {w}: {:.1} ms", t0.elapsed().as_secs_f64() * 1e3) };
    let units = chunks_of(&inp.links, 256 << 10);
    // endpoints of every valid link line, in line order: (hash, ref); an edge is
    // two consecutive endpoints
    let parts: Vec<Vec<(u64, u64)>> = units
        .par_iter()
        .map(|&(f, s, e)| {
            let mut v = Vec::with_capacity((e - s) / 10);
            for_records::<2>(f, s, e, |p, line, fs| {
                if let Some(fs) = fs {
                    let (a0, a1) = fs.get(0);
                    let (b0, b1) = fs.get(1);
                    if a1 > a0 && b1 > b0 {
                        v.push((h64(&line[a0..a1]), mkref(f, p + a0, a1 - a0)));
                        v.push((h64(&line[b0..b1]), mkref(f, p + b0, b1 - b0)));
                    }
                }
            });
            v
        })
        .collect();
    lt("parsed");
    // scatter endpoints by the hash's top bits: (hash, ref, endpoint index)
    const PB: u32 = PM_PARTS_BITS;
    let np = PM_PARTS;
    let part = |h: u64| (h >> (64 - PB)) as usize;
    let mut ubase = vec![0usize; parts.len() + 1];
    for (u, v) in parts.iter().enumerate() {
        ubase[u + 1] = ubase[u] + v.len();
    }
    let total = ubase[parts.len()];
    let counts: Vec<Vec<u32>> = parts
        .par_iter()
        .map(|v| {
            let mut c = vec![0u32; np];
            for &(h, _) in v {
                c[part(h)] += 1;
            }
            c
        })
        .collect();
    // offsets[u][p]: where unit u writes its partition-p endpoints
    let mut pstart = vec![0usize; np + 1];
    let mut offsets: Vec<Vec<usize>> = vec![vec![0usize; np]; parts.len()];
    {
        let mut acc = 0usize;
        for p in 0..np {
            pstart[p] = acc;
            for u in 0..parts.len() {
                offsets[u][p] = acc;
                acc += counts[u][p] as usize;
            }
        }
        pstart[np] = acc;
    }
    let mut scat: Vec<(u64, u64, u32)> = Vec::with_capacity(total);
    unsafe { scat.set_len(total) };
    let sbase = scat.as_mut_ptr() as usize;
    parts.par_iter().zip(offsets.par_iter_mut()).enumerate().for_each(|(u, (v, off))| {
        for (k, &(h, r)) in v.iter().enumerate() {
            let p = part(h);
            unsafe { *(sbase as *mut (u64, u64, u32)).add(off[p]) = (h, r, (ubase[u] + k) as u32) };
            off[p] += 1;
        }
    });
    drop(parts);
    lt("scattered");
    // per partition: sort by hash, number the distinct hashes
    let scat_parts: Vec<&mut [(u64, u64, u32)]> = {
        let mut rest: &mut [(u64, u64, u32)] = &mut scat[..];
        let mut out = Vec::with_capacity(np);
        for p in 0..np {
            let (a, b) = rest.split_at_mut(pstart[p + 1] - pstart[p]);
            out.push(a);
            rest = b;
        }
        out
    };
    let mut ids: Vec<u32> = vec![0; total];
    let ids_base = ids.as_mut_ptr() as usize;
    let local: Vec<Vec<(u64, u64)>> = scat_parts
        .into_par_iter()
        .map(|sp| {
            sp.sort_unstable_by_key(|x| x.0);
            let mut uq: Vec<(u64, u64)> = Vec::with_capacity(sp.len() / 2 + 1);
            let mut prev = 0u64;
            for &(h, r, at) in sp.iter() {
                if uq.is_empty() || h != prev {
                    uq.push((h, r));
                    prev = h;
                }
                // local id for now; made global below
                unsafe { *(ids_base as *mut u32).add(at as usize) = (uq.len() - 1) as u32 };
            }
            uq
        })
        .collect();
    let mut gbase = vec![0u32; np + 1];
    for p in 0..np {
        gbase[p + 1] = gbase[p] + local[p].len() as u32;
    }
    let n = gbase[np] as usize;
    let uniq: Vec<(u64, u64)> = local.concat();
    // make the ids global: add each partition's base
    (0..np).into_par_iter().for_each(|p| {
        for &(_, _, at) in &scat[pstart[p]..pstart[p + 1]] {
            unsafe { *(ids_base as *mut u32).add(at as usize) += gbase[p] };
        }
    });
    let edges: Vec<(u32, u32)> = (0..total / 2).into_par_iter().with_min_len(4096).map(|e| (ids[2 * e], ids[2 * e + 1])).collect();
    drop(scat);
    drop(ids);
    lt("edges");
    // lock-free union-find: a root only ever links to a smaller root
    let parent: Vec<AtomicU32> = (0..n as u32).into_par_iter().map(AtomicU32::new).collect();
    #[inline(always)]
    fn find(p: &[AtomicU32], mut x: u32) -> u32 {
        loop {
            let px = p[x as usize].load(Ordering::Relaxed);
            if px == x {
                return x;
            }
            let gx = p[px as usize].load(Ordering::Relaxed);
            if gx != px {
                let _ = p[x as usize].compare_exchange_weak(px, gx, Ordering::Relaxed, Ordering::Relaxed);
            }
            x = gx;
        }
    }
    edges.par_iter().with_min_len(2048).for_each(|&(a, b)| {
        let (mut a, mut b) = (a, b);
        loop {
            a = find(&parent, a);
            b = find(&parent, b);
            if a == b {
                return;
            }
            let (hi, lo) = if a > b { (a, b) } else { (b, a) };
            if parent[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    });
    drop(edges);
    let root: Vec<u32> = (0..n as u32).into_par_iter().with_min_len(4096).map(|i| find(&parent, i)).collect();
    drop(parent);
    lt("uf");
    // smallest name per component
    let keys: Vec<(u128, u32, u64)> = uniq
        .par_iter()
        .with_min_len(1024)
        .map(|&(_, r)| {
            let (k, l) = name_key(deref(r));
            (k, l, r)
        })
        .collect();
    let best: Vec<AtomicU32> = (0..n as u32).into_par_iter().map(AtomicU32::new).collect();
    (0..n).into_par_iter().with_min_len(2048).for_each(|i| {
        let r = root[i] as usize;
        if r == i {
            return;
        }
        let mut cur = best[r].load(Ordering::Relaxed);
        while name_less(keys[i], keys[cur as usize]) {
            match best[r].compare_exchange_weak(cur, i as u32, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    });
    lt("best");
    let mut comp: Vec<u32> = vec![u32::MAX; n];
    let mut names: Vec<u64> = Vec::new();
    for i in 0..n {
        if root[i] as usize == i {
            comp[i] = names.len() as u32;
            names.push(keys[best[i].load(Ordering::Relaxed) as usize].2);
        }
    }
    let entries: Vec<(u64, u64)> =
        (0..n).into_par_iter().with_min_len(4096).map(|i| (uniq[i].0, LINKED | comp[root[i] as usize] as u64)).collect();
    lt("names");
    // `uniq` is sorted by hash (partitions in order, sorted inside), so `entries` is too
    let map = PersonMap::build(&entries);
    lt("map");
    std::mem::forget(entries);
    std::mem::forget(keys);
    Persons { map, names }
}

// ---------------------------------------------------------------- records

/// A surviving event, keyed by person. 24 bytes.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ev2 {
    key: u64,
    uref: u64,
    ts: u32,
    pc: u32,
}

/// An event inside a person partition, once grouped: the person is known by
/// position, so only time and purchase remain. 8 bytes.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ev1 {
    ts: u32,
    pc: u32,
}

/// A click inside a person partition, once grouped. 16 bytes.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ck1 {
    cref: u64,
    ts: u32,
    camp: u32,
}

/// A valid click, keyed by person.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ck {
    key: u64,
    cref: u64,
    ts: u32, // ms since window start - 7 days (CLICK_REACH): never negative
    camp: u32,
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
    /// [day * (ncamp + 1) + camp] -> (sessions | purchasing sessions << 32, revenue), credited
    pub credit: Vec<(u64, i64)>,
    /// [day * (ncamp + 1) + camp] -> assisted sessions
    pub assist: Vec<u32>,
}

struct Ctx<'a> {
    ndays: usize,
    nbins: usize,
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
    evs_sorted: Vec<Ev1>,
    cks_sorted: Vec<Ck1>,
    // per grouped person: key and the ref of one of its events' user IDs
    pkey: Vec<u64>,
    puref: Vec<u64>,
    bin_cnt: Vec<u32>,
    tmp_ev: Vec<Ev1>,
    tmp_id: Vec<u32>,
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
            evs_sorted: Vec::new(),
            cks_sorted: Vec::new(),
            pkey: Vec::new(),
            puref: Vec::new(),
            bin_cnt: Vec::new(),
            tmp_ev: Vec::new(),
            tmp_id: Vec::new(),
        }
    }
}

/// Group one person bucket's events and clicks by person, then sessionize.
/// Local person id of `key` in the grouping table, inserting a new person.
#[inline(always)]
fn group_id(sc: &mut Scratch, key: u64, cap: &mut usize) -> u32 {
    let slot_of = |k: u64, mask: usize| (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & mask;
    let mask = *cap - 1;
    let mut s = slot_of(key, mask);
    let id = loop {
        let t = unsafe { sc.table.get_unchecked_mut(s) };
        if t.1 == u32::MAX {
            *t = (key, sc.ev_cnt.len() as u32);
            sc.used.push(s as u32);
            sc.ev_cnt.push(0);
            break t.1;
        }
        if t.0 == key {
            break t.1;
        }
        s = (s + 1) & mask;
    };
    if sc.used.len() * 2 > *cap {
        // grow: clear the used slots, re-insert every person at twice the size
        let entries: Vec<(u64, u32)> = sc.used.iter().map(|&u| sc.table[u as usize]).collect();
        for &u in &sc.used {
            sc.table[u as usize] = (0, u32::MAX);
        }
        *cap *= 2;
        let mask = *cap - 1;
        if sc.table.len() < *cap {
            sc.table.resize(*cap, (0, u32::MAX));
        }
        sc.used.clear();
        for (k, id) in entries {
            let mut s = slot_of(k, mask);
            while sc.table[s].1 != u32::MAX {
                s = (s + 1) & mask;
            }
            sc.table[s] = (k, id);
            sc.used.push(s as u32);
        }
    }
    id
}

/// Group one person partition's live events and clicks by person, then
/// sessionize. Gathering the records from every worker's stream, numbering
/// persons and counting half-hour bins happen in one pass.
#[allow(clippy::too_many_arguments)]
fn process_bucket(ctx: &Ctx, agg: &mut Agg, sc: &mut Scratch, b: usize, sinks: &[flow::Sink], dead: &flow::Dead, evb: &mut Vec<Ev1>, ckb: &mut Vec<Ck1>) {
    let n: usize = sinks.iter().map(|s| s.ev.len(b)).sum();
    if n == 0 {
        return;
    }
    // persons are ~30x fewer than events: the table starts small (cache
    // resident) and doubles when half full
    let mut cap = (n / 8).next_power_of_two().max(256);
    if sc.table.len() < cap {
        sc.table.resize(cap, (0, u32::MAX));
    }
    sc.used.clear();
    sc.ev_cnt.clear();
    sc.pkey.clear();
    sc.puref.clear();
    sc.ev_local.clear();
    sc.ev_local.reserve(n);
    let nb = ctx.nbins;
    sc.bin_cnt.clear();
    sc.bin_cnt.resize(nb + 1, 0);
    evb.clear();
    evb.reserve(n);
    for (t, s) in sinks.iter().enumerate() {
        let mut i = 0usize;
        for sl in s.ev.slices(b) {
            for (j, e) in sl.iter().enumerate() {
                if j % 4 == 0 {
                    // records were streamed out of the cache: fetch ahead (the
                    // hardware prefetcher restarts at every 4 KiB)
                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        std::arch::x86_64::_mm_prefetch(sl.as_ptr().wrapping_add(j + 24) as *const i8, std::arch::x86_64::_MM_HINT_T0)
                    };
                }
                if (dead.word(t, b, i / 64) >> (i % 64)) & 1 == 0 {
                    let id = group_id(sc, e.key, &mut cap);
                    if id as usize == sc.pkey.len() {
                        sc.pkey.push(e.key);
                        sc.puref.push(e.uref);
                    }
                    unsafe {
                        *sc.ev_cnt.get_unchecked_mut(id as usize) += 1;
                        *sc.bin_cnt.get_unchecked_mut(bin_of(e.ts) + 1) += 1;
                        let len = evb.len();
                        evb.as_mut_ptr().add(len).write(Ev1 { ts: e.ts, pc: e.pc });
                        evb.set_len(len + 1);
                        let len = sc.ev_local.len();
                        sc.ev_local.as_mut_ptr().add(len).write(id);
                        sc.ev_local.set_len(len + 1);
                    }
                }
                i += 1;
            }
        }
    }
    let n = evb.len();
    if n == 0 {
        return;
    }
    let mask = cap - 1;
    let slot_of = |k: u64, mask: usize| (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & mask;
    let evin: &[Ev1] = evb;
    let np = sc.ev_cnt.len();
    sc.ck_cnt.clear();
    sc.ck_cnt.resize(np, 0);
    sc.ck_local.clear();
    ckb.clear();
    for s in sinks {
        for sl in s.ck.slices(b) {
            for c in sl {
                let mut s = slot_of(c.key, mask);
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
                // clicks of persons without events never matter
                if id != u32::MAX {
                    sc.ck_cnt[id as usize] += 1;
                    ckb.push(Ck1 { cref: c.cref, ts: c.ts, camp: c.camp });
                    sc.ck_local.push(id);
                }
            }
        }
    }
    let ckin: &[Ck1] = ckb;
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
    // events by person, and inside a person by half-hour bin (two stable
    // counting passes, no comparison sort): within one bin no two events can
    // be more than 30 minutes apart, so a session never splits inside a bin
    for b in 0..nb {
        sc.bin_cnt[b + 1] += sc.bin_cnt[b];
    }
    sc.tmp_ev.clear();
    sc.tmp_ev.reserve(n);
    sc.tmp_id.clear();
    sc.tmp_id.reserve(n);
    unsafe {
        sc.tmp_ev.set_len(n);
        sc.tmp_id.set_len(n);
        for (e, &id) in evin.iter().zip(&sc.ev_local) {
            let at = sc.bin_cnt.get_unchecked_mut(bin_of(e.ts));
            *sc.tmp_ev.get_unchecked_mut(*at as usize) = *e;
            *sc.tmp_id.get_unchecked_mut(*at as usize) = id;
            *at += 1;
        }
    }
    let mut ev_pos = sc.ev_cnt.clone();
    unsafe {
        for (e, &id) in sc.tmp_ev.iter().zip(&sc.tmp_id) {
            let at = ev_pos.get_unchecked_mut(id as usize);
            *sc.evs_sorted.get_unchecked_mut(*at as usize) = *e;
            *at += 1;
        }
    }
    let mut ck_pos = sc.ck_cnt.clone();
    for (c, &id) in ckin.iter().zip(&sc.ck_local) {
        let at = &mut ck_pos[id as usize];
        sc.cks_sorted[*at as usize] = *c;
        *at += 1;
    }
    let evs = std::mem::take(&mut sc.evs_sorted);
    let cks = std::mem::take(&mut sc.cks_sorted);
    let mut evs = evs;
    let mut cks = cks;
    for pid in 0..np {
        let (es, ee) = (sc.ev_cnt[pid] as usize, ev_pos[pid] as usize);
        let (cs, ce) = (sc.ck_cnt[pid] as usize, ck_pos[pid] as usize);
        let ev = &mut evs[es..ee];
        let ck = &mut cks[cs..ce];
        if ck.len() > 1 {
            ck.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| deref(a.cref).cmp(deref(b.cref))));
        }
        person_sessions(ctx, sc.pkey[pid], sc.puref[pid], ev, ck, agg, sc);
    }
    sc.evs_sorted = evs;
    sc.cks_sorted = cks;
}

/// Half-hour bin of a timestamp (ms since window start).
#[inline(always)]
fn bin_of(ts: u32) -> usize {
    (ts / SESSION_GAP as u32) as usize
}

/// One run of a person's events that share a half-hour bin.
struct Run {
    end: usize,
    min: u32,
    max: u32,
    n: u32,
    rev: i64,
    purch: bool,
}

#[inline(always)]
fn run_at(evs: &[Ev1], k: usize) -> Run {
    let b = bin_of(evs[k].ts);
    let mut r = Run { end: k, min: u32::MAX, max: 0, n: 0, rev: 0, purch: false };
    while r.end < evs.len() && bin_of(evs[r.end].ts) == b {
        let e = &evs[r.end];
        r.min = r.min.min(e.ts);
        r.max = r.max.max(e.ts);
        r.n += 1;
        if e.pc >= 256 {
            r.rev += (e.pc >> 8) as i64;
            r.purch = true;
        }
        r.end += 1;
    }
    r
}

/// `evs`: one person's events grouped by half-hour bin, bins ascending (order
/// inside a bin is arbitrary). `cks`: the person's clicks by (ts, click_id).
fn person_sessions(ctx: &Ctx, key: u64, uref: u64, evs: &[Ev1], cks: &[Ck1], agg: &mut Agg, sc: &mut Scratch) {
    let name = if key & LINKED != 0 { ctx.persons.names[(key & !LINKED) as usize] } else { uref };
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day: i64 = i64::MIN;
    let (mut d_sess, mut d_evs, mut d_rev) = (0u32, 0u32, 0i64);
    let mut d_purch = false;
    let mut k = 0usize;
    let mut r = run_at(evs, 0);
    while k < evs.len() {
        let start = r.min as i64;
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
        // the session: this run and every following run within 30 minutes
        let mut n = 0u32;
        let mut rev = 0i64;
        let mut purch = false;
        loop {
            n += r.n;
            rev += r.rev;
            if r.purch {
                purch = true;
                for e in &evs[k..r.end] {
                    if e.pc >= 256 {
                        sc.cat[(e.pc & 0xFF) as usize] += (e.pc >> 8) as i64;
                    }
                }
            }
            let last = r.max as i64;
            k = r.end;
            if k == evs.len() {
                break;
            }
            r = run_at(evs, k);
            if r.min as i64 - last > SESSION_GAP {
                break;
            }
        }
        // attribution window [start - 7d, start]
        // click times are shifted by +CLICK_REACH: ts <= start and ts >= start - 7d
        while hi < cks.len() && (cks[hi].ts as i64) <= start + CLICK_REACH {
            let c = cks[hi].camp as usize;
            if sc.count[c] == 0 {
                sc.pos[c] = sc.active.len() as u32;
                sc.active.push(c as u32);
            }
            sc.count[c] += 1;
            hi += 1;
        }
        while lo < hi && (cks[lo].ts as i64) < start {
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
        slot.0 += 1 | ((purch as u64) << 32);
        slot.1 += rev;
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
    if let Ok(mode) = std::env::var("SS_MICRO") {
        // deterministic micro-benchmarks (run under cachegrind)
        let inp = open_all(&data);
        let units = chunks(&inp.events);
        let mut acc = 0u64;
        let mut lines = 0u64;
        if mode == "s1" || mode == "nl" {
            let mut bm = Bitmaps::default();
            for &(f, s, e) in &units {
                let d = file_bytes(f);
                let mut a = s;
                while a < e {
                    let b = (a + (192 << 10)).min(e);
                    let words = (b - a).div_ceil(64);
                    let (x, y, z) = bm.prepare(words);
                    simd::stage1(&d[a..b], x, y, z);
                    if mode == "nl" {
                        for w in x.iter() {
                            let mut c = *w;
                            while c != 0 {
                                lines += 1;
                                acc = acc.wrapping_add(c.trailing_zeros() as u64);
                                c &= c - 1;
                            }
                        }
                    } else {
                        acc = acc.wrapping_add(x[0] ^ y[words - 1]);
                        lines += (b - a) as u64 / 77;
                    }
                    a = b;
                }
            }
            eprintln!("micro {mode}: lines {lines} acc {acc}");
            return;
        }
        if mode.starts_with('s') && mode != "s1" {
            let m = ["scan", "scanf", "sts", "sts2", "sing", "shash"].iter().position(|x| *x == mode).unwrap_or(0);
            for &(f, s, e) in &units {
                scan::scan_records::<7>(f, s, e, |p, line, fs| {
                    lines += 1;
                    if let Some(fs) = fs {
                        let v = match m {
                            1 => (0..7).map(|i| fs.get(line, i).len() as u64).sum::<u64>(),
                            2 => timestamp(fs.get(line, 2)).unwrap_or(0) as u64,
                            3 => parse::ts_fast_ni(fs.get(line, 2)).unwrap_or(0) as u64,
                            4 => { let mut m = parse::Memo::new(); parse::ingest_memo_ni(fs.get(line, 3), &mut m).unwrap_or(0) as u64 },
                            5 => h64_chk(fs.get(line, 0)).0 ^ h64(fs.get(line, 1)),
                            _ => fs.e[3] as u64 ^ p as u64,
                        };
                        acc = acc.wrapping_add(v);
                    }
                });
            }
            eprintln!("micro {mode}: lines {lines} acc {acc}");
            return;
        }
        for &(f, s, e) in &units {
            for_records::<7>(f, s, e, |p, line, fs| {
                lines += 1;
                if let Some(fs) = fs {
                    let fld = |i: usize| {
                    let (a, b) = fs.get(i);
                    unsafe { line.get_unchecked(a..b) }
                };
                    match mode.as_str() {
                        "tok" => acc = acc.wrapping_add(fs.get(3).1 as u64 ^ p as u64),
                        "ts" => acc = acc.wrapping_add(timestamp(fld(2)).unwrap_or(0) as u64 ^ parse::ingest(fld(3)).unwrap_or(0) as u64),
                        "ts1" => acc = acc.wrapping_add(timestamp(fld(2)).unwrap_or(0) as u64),
                        "ing" => acc = acc.wrapping_add(parse::ingest(fld(3)).unwrap_or(0) as u64),
                        "hash" => acc = acc.wrapping_add(h64_chk(fld(0)).0 ^ h64(fld(1))),
                        _ => {}
                    }
                }
            });
        }
        eprintln!("micro {mode}: lines {lines} acc {acc} scalar {}", SCALAR.load(Ordering::Relaxed));
        return;
    }
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
    let rss = std::env::var("SS_RSS").is_ok();
    let mut lap = |what: &'static str| {
        laps.push((what, t0.elapsed().as_secs_f64() * 1e3));
        if rss {
            let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
            let g = |k: &str| st.lines().find(|l| l.starts_with(k)).map(|l| l[k.len()..].trim().to_string()).unwrap_or_default();
            eprintln!("rss {what}: anon {} file {} shmem {}", g("RssAnon:"), g("RssFile:"), g("RssShmem:"));
        }
    };

    flow::WC.store(std::env::var("SS_WC").map_or(flow::WC_DEFAULT, |v| v == "1"), Ordering::Relaxed);
    READ.store(std::env::var("SS_READ").map_or(true, |v| v != "0"), Ordering::Relaxed);
    let inp = open_all(data);
    // partition counts follow the input size, so a partition's working set
    // stays about the same from the 20M practice set to the 60M final
    let ev_bytes: usize = inp.events.iter().map(|&f| file_bytes(f).len()).sum();
    let est_events = (ev_bytes / 75).max(1);
    let auto = |per: usize| (usize::BITS - 1 - (est_events / per).max(1).leading_zeros()).clamp(9, 12);
    let envbits = |k: &str, d: u32| std::env::var(k).ok().and_then(|v| v.parse::<u32>().ok()).unwrap_or(d).clamp(6, 14);
    // a copy record holds the person partition in 12 bits
    P2B.store(envbits("SS_P2B", auto(P2_PER)).min(12), Ordering::Relaxed);
    flow::PDB.store(envbits("SS_PDB", auto(PD_PER)), Ordering::Relaxed);
    lap("open");
    let (ws, we) = (inp.window_start, inp.window_end);
    let ev_units = chunks(&inp.events);
    let ck_units = chunks(&inp.clicks);
    // links first: every record below is routed straight to its person
    let (persons, lk) = rayon::join(|| links(&inp), || lookups(&inp));
    lap("links");
    let nthreads = rayon::current_num_threads();
    let nfiles = unsafe { &*FILES.load(Ordering::Relaxed) }.len();
    // room for every event's user ID, at twice a thread's share (spare virtual memory)
    let name_cap = est_events / nthreads * 32 + (1 << 20);
    let sinks: Vec<std::sync::Mutex<flow::Sink>> =
        (0..nthreads).map(|t| std::sync::Mutex::new(flow::Sink::new(nfiles + t, name_cap))).collect();
    let units: Vec<(bool, usize, usize, usize)> =
        ev_units.iter().map(|&(f, s, e)| (true, f, s, e)).chain(ck_units.iter().map(|&(f, s, e)| (false, f, s, e))).collect();
    units.par_iter().for_each(|&(is_ev, f, s, e)| {
        let t = rayon::current_thread_index().unwrap_or(0) % nthreads;
        let mut sink = sinks[t].lock().unwrap();
        if is_ev {
            flow::events_unit(f, s, e, &lk, &persons, ws, we, &mut sink, t as u64);
        } else {
            flow::clicks_unit(f, s, e, &lk, &persons, ws, we, &mut sink);
        }
    });
    let mut sinks: Vec<flow::Sink> = sinks.into_iter().map(|m| m.into_inner().unwrap()).collect();
    sinks.par_iter_mut().for_each(|s| s.finish());
    let names: Vec<&'static [u8]> = sinks.iter_mut().map(|s| &*std::mem::take(&mut s.names).leak()).collect();
    NAMES.store(Box::into_raw(Box::new(names)), Ordering::Release);
    lap("parsed");
    let dead = flow::Dead::new(&sinks);
    let wide = flow::wide_sorted(&mut sinks);
    (0..flow::pd()).into_par_iter().for_each(|b| flow::dedup_part(b, &sinks, &wide, &dead));
    lap("dedup");

    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncamp = lk.campaign_names.len();
    assert!(we - ws + CLICK_REACH < u32::MAX as i64, "window too long");
    let nbins = ((we - ws) as u64 / SESSION_GAP as u64) as usize + 1;
    let ctx = Ctx { ndays, nbins, ncamp, ncat: lk.categories.len(), ws, ws_day, persons: &persons };
    // exactly one accumulator per worker thread (rayon's fold would create one
    // per split: hundreds of 0.8 MB counter arrays and output files at 8 threads)
    let states: Vec<std::sync::Mutex<(Agg, Scratch, Vec<Ev1>, Vec<Ck1>)>> = (0..nthreads)
        .map(|_| {
            std::sync::Mutex::new((
                Agg { rows: Vec::new(), credit: vec![(0u64, 0i64); (ncamp + 1) * ndays], assist: vec![0u32; (ncamp + 1) * ndays] },
                Scratch::new(ncamp),
                Vec::new(),
                Vec::new(),
            ))
        })
        .collect();
    // largest partitions first, so a giant component never starts last
    let mut order: Vec<(usize, usize)> = (0..p2()).map(|b| (sinks.iter().map(|s| s.ev.len(b)).sum::<usize>(), b)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    order.par_iter().with_max_len(1).for_each(|&(_, b)| {
        let i = rayon::current_thread_index().unwrap_or(0) % nthreads;
        let mut guard = states[i].lock().unwrap();
        let (agg, sc, evb, ckb) = &mut *guard;
        process_bucket(&ctx, agg, sc, b, &sinks, &dead, evb, ckb);
    });
    let aggs: Vec<Agg> = states.into_iter().map(|m| m.into_inner().unwrap().0).collect();
    lap("sessionized");
    output::write(out, aggs, &lk, &ctx_info(&ctx, &lk));
    lap("written");
    if leak {
        if std::env::var("SS_PFREE").is_ok_and(|v| v == "1") {
            // release the big mappings from all threads at once, so the
            // single-threaded teardown at exit has little left to do
            let files: Box<Vec<Mmap>> = unsafe { Box::from_raw(FILES.swap(std::ptr::null_mut(), Ordering::AcqRel)) };
            rayon::join(|| sinks.into_par_iter().for_each(drop), || files.into_par_iter().for_each(drop));
            lap("released");
        } else {
            // the process is about to exit: skip unmapping gigabytes of buffers
            std::mem::forget(sinks);
        }
        std::mem::forget(dead);
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

    use crate::tests_support::{install, naive};

    #[test]
    fn tokenizer_matches_scalar() {
        let _g = crate::tests_support::LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
                for_records::<4>(0, w[0], w[1], |p, _line, fs| got.push((p, fs.map(|f| f.pairs()))));
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

#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;
    /// Tests that install input files must not run concurrently.
    pub static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub fn install(bytes: &[u8]) {
        let mut m = memmap2::MmapMut::map_anon(bytes.len().max(1)).unwrap();
        m[..bytes.len()].copy_from_slice(bytes);
        let maps = vec![m.make_read_only().unwrap()];
        FILES.store(Box::into_raw(Box::new(maps)), Ordering::Release);
        LENS.store(Box::into_raw(Box::new(vec![bytes.len()])), Ordering::Release);
    }

    pub fn naive<const N: usize>(d: &[u8]) -> Vec<(usize, Option<Vec<(u32, u32)>>)> {
        let mut out = Vec::new();
        let mut p = match d.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => return out,
        };
        while p < d.len() {
            let end = d[p..].iter().position(|&c| c == b'\n').map_or(d.len(), |i| p + i);
            out.push((p, split::<N>(&d[p..end]).map(|f| f.pairs())));
            p = end + 1;
        }
        out
    }

}
