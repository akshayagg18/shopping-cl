//! Second-generation CSV tokenizer. One fused pass per 64-byte word builds
//! the newline bitmap, a bitmap of quoting violations, and a flat list of the
//! positions of every separator (comma or newline). A line is then: the next
//! newline from the bitmap, a single compare that its N-th separator is that
//! newline, one window test of the violation bitmap, and N field bounds read
//! straight from the separator list.
//!
//! Quoting (rules.md): a field is either free of quotes or wholly quoted. With
//! `inside` the running parity of quotes since the line start, that is exactly
//! - no separator inside quotes (this also catches an unbalanced line, whose
//!   newline is then inside),
//! - an opening quote is preceded by a separator (or the line start),
//! - a closing quote is followed by a separator.

use super::*;

/// Raw field ends (each field's separator), relative to the line start. A
/// field is unquoted on access: the line is valid, so a field that starts with
/// a quote is wholly quoted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Flds<const N: usize> {
    pub e: [u32; N],
}

impl<const N: usize> Flds<N> {
    /// (start, end) of field i without its quotes.
    #[inline(always)]
    pub fn span(&self, line: &[u8], i: usize) -> (usize, usize) {
        let a = if i == 0 { 0 } else { self.e[i - 1] as usize + 1 };
        let b = self.e[i] as usize;
        // a field before the last is followed by its separator inside the line,
        // so its first byte (or that separator) is always readable
        let q = if i + 1 < N { (unsafe { *line.get_unchecked(a) } == b'"') as usize } else { (b > a && unsafe { *line.get_unchecked(a) } == b'"') as usize };
        (a + q, b - q)
    }
    #[inline(always)]
    pub fn get<'a>(&self, line: &'a [u8], i: usize) -> &'a [u8] {
        let (a, b) = self.span(line, i);
        unsafe { line.get_unchecked(a..b) }
    }
    #[allow(dead_code)]
    pub fn pairs(&self, line: &[u8]) -> Vec<(u32, u32)> {
        (0..N).map(|i| { let (a, b) = self.span(line, i); (a as u32, b as u32) }).collect()
    }
}

#[derive(Default)]
struct ScanBuf {
    nl: Vec<u64>,
    bad: Vec<u64>,
    sep: Vec<u32>,
}

thread_local! {
    static SCANBUF: std::cell::RefCell<ScanBuf> = std::cell::RefCell::new(ScanBuf::default());
}

/// Quote parity with a reset after every newline (only needed when a newline
/// falls inside quotes, i.e. an unbalanced line: rare).
#[cold]
#[inline(never)]
fn inside_reset(q: u64, nl: u64, carry: u64) -> u64 {
    let mut res = 0u64;
    let mut c = carry;
    let mut start = 0u32;
    let mut rem = nl;
    loop {
        let end = if rem == 0 { 64 } else { rem.trailing_zeros() + 1 };
        let hi = if end == 64 { !0u64 } else { (1u64 << end) - 1 };
        let seg = hi & (!0u64 << start);
        let px = prefix_xor64(q & seg) ^ c;
        res |= px & seg;
        if end >= 64 {
            break;
        }
        c = 0;
        rem &= rem - 1;
        start = end;
    }
    res
}

#[inline(always)]
fn masks64(b: &[u8]) -> (u64, u64, u64) {
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

/// Carried state of stage 1 between words.
struct S1 {
    qcarry: u64,
    sep_carry: u64,
    close_carry: u64,
    nsep: usize,
}

/// One 64-byte word of stage 1.
#[inline(always)]
unsafe fn s1_word(st: &mut S1, nl: u64, cm: u64, q: u64, w: usize, nlo: *mut u64, bado: *mut u64, sepo: *mut u32) {
    unsafe {
        let sep = nl | cm;
        let mut inside = prefix_xor64(q) ^ st.qcarry;
        if inside & nl != 0 {
            inside = inside_reset(q, nl, st.qcarry);
        }
        // after a newline the parity restarts: a newline in bit 63 carries nothing
        st.qcarry = 0u64.wrapping_sub((inside & !nl) >> 63);
        let open = q & inside;
        let close = q & !inside;
        let prev_sep = (sep << 1) | st.sep_carry;
        st.sep_carry = sep >> 63;
        let close_next = (close << 1) | st.close_carry;
        st.close_carry = close >> 63;
        *nlo.add(w) = nl;
        *bado.add(w) = (inside & sep) | (open & !prev_sep) | (close_next & !sep);
        // flatten separator positions: 8 unconditional stores cover most words
        let base = (w * 64) as u32;
        let cnt = sep.count_ones() as usize;
        let o = sepo.add(st.nsep);
        let mut x = sep;
        for k in 0..8 {
            std::ptr::write_volatile(o.add(k), base + x.trailing_zeros());
            x &= x.wrapping_sub(1);
        }
        if cnt > 8 {
            let mut k = 8;
            while x != 0 {
                *o.add(k) = base + x.trailing_zeros();
                x &= x.wrapping_sub(1);
                k += 1;
            }
        }
        st.nsep += cnt;
    }
}

/// Stage 1 over `d` (starting 64-aligned, first line starting at bit `first`).
/// `eof`: `d` ends at the end of the file; a final line without '\n' gets a
/// virtual one there. Returns the number of bitmap words.
/// SS_A512: 1 = stage-1 masks with AVX-512BW/VL compares on 256-bit vectors
/// (no frequency licence change), 2 = on 512-bit vectors; only if the CPU has them.
pub static A512: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

pub fn set_a512(v: u8) {
    #[cfg(target_arch = "x86_64")]
    {
        let ok = std::arch::is_x86_feature_detected!("avx512bw") && std::arch::is_x86_feature_detected!("avx512vl");
        A512.store(if ok { v.min(2) } else { 0 }, std::sync::atomic::Ordering::Relaxed);
    }
}

#[inline(always)]
unsafe fn masks_avx2(p: *const u8) -> (u64, u64, u64) {
    masks64(unsafe { std::slice::from_raw_parts(p, 64) })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512vl,avx2,bmi1,bmi2,popcnt,pclmulqdq")]
#[inline]
unsafe fn masks_k256(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let c = _mm256_set1_epi8(b',' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let nl = (_mm256_cmpeq_epi8_mask(v0, n) as u64) | ((_mm256_cmpeq_epi8_mask(v1, n) as u64) << 32);
        let cm = (_mm256_cmpeq_epi8_mask(v0, c) as u64) | ((_mm256_cmpeq_epi8_mask(v1, c) as u64) << 32);
        let qq = (_mm256_cmpeq_epi8_mask(v0, q) as u64) | ((_mm256_cmpeq_epi8_mask(v1, q) as u64) << 32);
        (nl, cm, qq)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vl,avx2,bmi1,bmi2,popcnt,pclmulqdq")]
#[inline]
unsafe fn masks_k512(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let v = _mm512_loadu_si512(p as *const __m512i);
        (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
        )
    }
}

macro_rules! stage1_body {
    ($m:ident, $d:ident, $first:ident, $eof:ident, $buf:ident) => {{
        let (d, first, eof, buf) = ($d, $first, $eof, $buf);

    // at EOF one more (possibly empty) partial word holds the virtual newline
    let words = if eof { d.len() / 64 + 1 } else { d.len().div_ceil(64) };
    for v in [&mut buf.nl, &mut buf.bad] {
        v.clear();
        v.reserve(words + 3);
    }
    buf.sep.clear();
    buf.sep.reserve(d.len() + 128);
    let (nlo, bado, sepo) = (buf.nl.as_mut_ptr(), buf.bad.as_mut_ptr(), buf.sep.as_mut_ptr());
    let mut st = S1 { qcarry: 0, sep_carry: 1, close_carry: 0, nsep: 0 };
    let full = d.len() / 64;
    let mut qmask = !0u64 << first;
    unsafe {
        let dp = d.as_ptr();
        for w in 0..full.min(words) {
            let (nl, cm, q) = $m(dp.add(w * 64));
            s1_word(&mut st, nl, cm, q & qmask, w, nlo, bado, sepo);
            qmask = !0;
        }
        if words > full {
            let mut tail = [0u8; 64];
            let rest = &d[full * 64..];
            tail[..rest.len()].copy_from_slice(rest);
            let (nl, cm, q) = $m(tail.as_ptr());
            let nl = if eof { nl | (1u64 << rest.len()) } else { nl };
            s1_word(&mut st, nl, cm, q & qmask, full, nlo, bado, sepo);
        }
        for w in words..words + 3 {
            *nlo.add(w) = 0;
            *bado.add(w) = 0;
        }
        buf.nl.set_len(words + 3);
        buf.bad.set_len(words + 3);
        buf.sep.set_len(st.nsep);
    }
    words
    }};
}

fn stage1(d: &[u8], first: usize, eof: bool, buf: &mut ScanBuf) -> usize {
    #[cfg(target_arch = "x86_64")]
    match A512.load(std::sync::atomic::Ordering::Relaxed) {
        1 => return unsafe { stage1_k256(d, first, eof, buf) },
        2 => return unsafe { stage1_k512(d, first, eof, buf) },
        _ => {}
    }
    stage1_body!(masks_avx2, d, first, eof, buf)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512vl,avx2,bmi1,bmi2,popcnt,pclmulqdq")]
unsafe fn stage1_k256(d: &[u8], first: usize, eof: bool, buf: &mut ScanBuf) -> usize {
    stage1_body!(masks_k256, d, first, eof, buf)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vl,avx2,bmi1,bmi2,popcnt,pclmulqdq")]
unsafe fn stage1_k512(d: &[u8], first: usize, eof: bool, buf: &mut ScanBuf) -> usize {
    stage1_body!(masks_k512, d, first, eof, buf)
}

/// Any bit set in [from, to] (inclusive) of a bitmap with 3 words of padding.
#[inline(always)]
fn any_bits(bm: &[u64], from: usize, to: usize) -> bool {
    // violations are rare: two whole words are usually clean
    let (wa, wb) = (from / 64, to / 64);
    if wb <= wa + 1 && (unsafe { *bm.get_unchecked(wa) | *bm.get_unchecked(wa + 1) }) == 0 {
        return false;
    }
    let len = to - from + 1;
    if len <= 57 {
        let v = unsafe { std::ptr::read_unaligned((bm.as_ptr() as *const u8).add(from / 8) as *const u64) } >> (from % 8);
        v & ((1u64 << len) - 1) != 0
    } else {
        let (a, b) = (from / 64, to / 64);
        let mut acc = 0u64;
        for w in a..=b {
            let mut x = bm[w];
            if w == a {
                x &= !0u64 << (from % 64);
            }
            if w == b && to % 64 != 63 {
                x &= (1u64 << (to % 64 + 1)) - 1;
            }
            acc |= x;
        }
        acc != 0
    }
}

/// A line that runs past the bitmap: the scalar splitter (rare, kept out of line).
#[cold]
#[inline(never)]
fn long_line<const N: usize>(data: &[u8], p: usize, fl: &mut Flds<N>) -> (usize, bool) {
    let n = data.len();
    let end = match memchr(b'\n', &data[p..]) {
        Some(i) => p + i,
        None => n,
    };
    match parse::split::<N>(&data[p..end]) {
        Some(fs) => {
            fl.e = fs.ends;
            (end, true)
        }
        None => (end, false),
    }
}

/// `line_fields` for a line of 128..=191 bytes (192 readable): three 64-bit words.
#[inline(never)]
unsafe fn line_fields3<const N: usize>(ptr: *const u8, len: usize, fl: &mut Flds<N>) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let mm = |o: usize, k: __m256i| {
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.add(o) as *const __m256i), k)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.add(o + 32) as *const __m256i), k)) as u32 as u64;
            a | (b << 32)
        };
        let kc = _mm256_set1_epi8(b',' as i8);
        let kq = _mm256_set1_epi8(b'"' as i8);
        debug_assert!((128..192).contains(&len));
        // bits [0, len] (the newline included): words 0 and 1 whole, word 2 up to len - 128
        let m2 = (2u64 << (len - 128)).wrapping_sub(1);
        let nl2 = 1u64 << (len - 128);
        let sep = [mm(0, kc), mm(64, kc), (mm(128, kc) & m2) | nl2];
        let q = [mm(0, kq), mm(64, kq), mm(128, kq) & m2];
        let mut carry_in = 0u64;
        let mut prev_sep = 1u64;
        let mut prev_close = 0u64;
        let mut bad = 0u64;
        let mut count = 0u32;
        for w in 0..3 {
            let inside = prefix_xor64(q[w]) ^ carry_in;
            carry_in = 0u64.wrapping_sub(inside >> 63);
            let prev = (sep[w] << 1) | prev_sep;
            prev_sep = sep[w] >> 63;
            let close = q[w] & !inside;
            let cn = (close << 1) | prev_close;
            prev_close = close >> 63;
            let m = if w == 2 { m2 } else { !0u64 };
            bad |= ((inside & sep[w]) | (q[w] & inside & !prev) | (cn & !sep[w])) & m;
            count += sep[w].count_ones();
        }
        if count as usize != N || bad != 0 {
            return false;
        }
        let mut k = 0usize;
        for w in 0..3 {
            let mut x = sep[w];
            while x != 0 {
                fl.e[k] = (w * 64) as u32 + x.trailing_zeros();
                x &= x - 1;
                k += 1;
            }
        }
        true
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let _ = (ptr, len, fl);
        unreachable!("line_fields needs AVX2")
    }
}


/// A line of `len` < 96 bytes at `ptr` (96 bytes readable, newline at
/// `len`): the field ends if valid with exactly N fields. Same rules as stage
/// 1: no separator inside quotes, every opening quote follows a separator (or
/// the line start), every closing quote precedes one (or the line end).
#[inline(always)]
unsafe fn line_fields<const N: usize, const L: usize>(ptr: *const u8, len: usize, fl: &mut Flds<N>) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let v0 = _mm256_loadu_si256(ptr as *const __m256i);
        let v1 = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
        let v2 = _mm256_loadu_si256(ptr.add(64) as *const __m256i);
        // L = 4: a fourth load covers lines of up to 127 bytes
        let v3 = if L == 4 { _mm256_loadu_si256(ptr.add(96) as *const __m256i) } else { _mm256_setzero_si256() };
        let mm = |v: __m256i, k: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, k)) as u32 as u64;
        let kc = _mm256_set1_epi8(b',' as i8);
        let kq = _mm256_set1_epi8(b'"' as i8);
        // bits [0, len] of the 96 (the newline included), without branches
        let m_lo = if len >= 63 { !0u64 } else { (2u64 << len) - 1 };
        let m_hi = if len >= 64 { (2u64 << (len - 64)).wrapping_sub(1) } else { 0 };
        let nl_lo = if len < 64 { 1u64 << (len & 63) } else { 0 };
        let nl_hi = if len >= 64 { 1u64 << (len & 63) } else { 0 };
        let sep_lo = ((mm(v0, kc) | (mm(v1, kc) << 32)) & m_lo) | nl_lo;
        let sep_hi = ((mm(v2, kc) | if L == 4 { mm(v3, kc) << 32 } else { 0 }) & m_hi) | nl_hi;
        let q_lo = (mm(v0, kq) | (mm(v1, kq) << 32)) & m_lo;
        let q_hi = (mm(v2, kq) | if L == 4 { mm(v3, kq) << 32 } else { 0 }) & m_hi;
        let in_lo = prefix_xor64(q_lo);
        let in_hi = prefix_xor64(q_hi) ^ 0u64.wrapping_sub(in_lo >> 63);
        let prev_lo = (sep_lo << 1) | 1;
        let prev_hi = (sep_hi << 1) | (sep_lo >> 63);
        let close_lo = q_lo & !in_lo;
        let close_hi = q_hi & !in_hi;
        let cn_lo = close_lo << 1;
        let cn_hi = (close_hi << 1) | (close_lo >> 63);
        let bad_lo = (in_lo & sep_lo) | (q_lo & in_lo & !prev_lo) | (cn_lo & !sep_lo);
        let bad_hi = (in_hi & sep_hi) | (q_hi & in_hi & !prev_hi) | (cn_hi & !sep_hi);
        if (sep_lo.count_ones() + sep_hi.count_ones()) as usize != N || ((bad_lo & m_lo) | (bad_hi & m_hi)) != 0 {
            return false;
        }
        let (mut a, mut b) = (sep_lo, sep_hi);
        let ca = a.count_ones() as usize;
        for i in 0..N {
            if i < ca {
                fl.e[i] = a.trailing_zeros();
                a &= a - 1;
            } else {
                fl.e[i] = 64 + b.trailing_zeros();
                b &= b.wrapping_sub(1);
            }
        }
        true
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let _ = (ptr, len, fl);
        unreachable!("line_fields needs AVX2")
    }
}

thread_local! {
    static NLBUF: std::cell::RefCell<Vec<u64>> = std::cell::RefCell::new(Vec::new());
}

/// Records starting in [s, e) (a file's header skipped), straight from the
/// mapping: a newline bitmap per 192 KiB window gives every line's start and
/// length up front (no chain from one line's end to the next line's loads),
/// then each line's fields come from three 32-byte loads. Lines near the
/// file's end and the rare line of 96+ bytes take the general paths.
#[inline(always)]
pub fn scan_lines<const N: usize>(f: usize, s: usize, e: usize, mut each: impl FnMut(usize, &[u8], Option<&Flds<N>>)) {
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        return scan_records::<N>(f, s, e, each);
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        if crate::READ.load(Ordering::Relaxed) || !crate::TOK1.load(Ordering::Relaxed) {
            return scan_records::<N>(f, s, e, each);
        }
        let d = file_bytes(f);
        let n = d.len();
        let mut p = if s == 0 {
            match memchr(b'\n', d) {
                Some(i) => i + 1,
                None => return,
            }
        } else if d[s - 1] == b'\n' {
            s
        } else {
            match memchr(b'\n', &d[s..]) {
                Some(i) => s + i + 1,
                None => return,
            }
        };
        // a line starting before fast_end has 192 readable bytes
        let fast_end = n.saturating_sub(192);
        let mut fl = Flds::<N> { e: [0; N] };
        NLBUF.with(|cell| {
            let mut nlv = cell.borrow_mut();
            const WIN: usize = 192 << 10;
            while p < e {
                if p >= fast_end {
                    return scan_records::<N>(f, p, e, &mut each);
                }
                // newline bitmap of [base, base + 64 * words), within fast_end + 96
                let base = p & !63;
                let lim = (e.min(fast_end) + 192).min(base + WIN + 256);
                let words = (lim - base) / 64;
                nlv.clear();
                nlv.reserve(words);
                unsafe {
                    let kn = _mm256_set1_epi8(b'\n' as i8);
                    let o = nlv.as_mut_ptr();
                    for w in 0..words {
                        let q = d.as_ptr().add(base + w * 64);
                        let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(q as *const __m256i), kn)) as u32 as u64;
                        let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(q.add(32) as *const __m256i), kn)) as u32 as u64;
                        *o.add(w) = a | (b << 32);
                    }
                    nlv.set_len(words);
                }
                let covered = base + words * 64;
                let mut w = (p - base) / 64;
                let mut cur = nlv[w] & (!0u64 << ((p - base) % 64));
                // lines whose newline lies in the bitmap
                loop {
                    while cur == 0 && w + 1 < words {
                        w += 1;
                        cur = nlv[w];
                    }
                    if cur == 0 {
                        break;
                    }
                    let end = base + w * 64 + cur.trailing_zeros() as usize;
                    cur &= cur - 1;
                    let len = end - p;
                    if p >= fast_end {
                        break;
                    }
                    let ok = if len < 96 {
                        unsafe { line_fields::<N, 3>(d.as_ptr().add(p), len, &mut fl) }
                    } else if len < 128 {
                        unsafe { line_fields::<N, 4>(d.as_ptr().add(p), len, &mut fl) }
                    } else if len < 192 {
                        unsafe { line_fields3::<N>(d.as_ptr().add(p), len, &mut fl) }
                    } else {
                        long_line::<N>(d, p, &mut fl).1
                    };
                    each(p, &d[p..end], if ok { Some(&fl) } else { None });
                    p = end + 1;
                    if p >= e {
                        return;
                    }
                }
                // no newline left in the bitmap: a line running past it
                if p < covered && p < fast_end {
                    let (end, ok) = long_line::<N>(d, p, &mut fl);
                    each(p, &d[p..end.min(n)], if ok { Some(&fl) } else { None });
                    p = end + 1;
                }
            }
        });
    }
}

/// Iterate records starting in [s, e) (a file's header skipped), split into N
/// fields: `each(line_start, line, fields)`, fields None for a malformed line.
#[inline(always)]
pub fn scan_records<const N: usize>(f: usize, s: usize, e: usize, mut each: impl FnMut(usize, &[u8], Option<&Flds<N>>)) {
    const SUB: usize = 192 << 10;
    let file = if crate::READ.load(Ordering::Relaxed) { Some(crate::open_input(f)) } else { None };
    let mut a = s;
    while a < e {
        let b = (a + SUB).min(e);
        scan_block::<N>(f, file.as_ref(), a, b, &mut each);
        a = b;
    }
}

thread_local! {
    static WINDOW: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::new());
}

#[inline(never)]
fn scan_block<const N: usize>(f: usize, file: Option<&std::fs::File>, s: usize, e: usize, each: &mut impl FnMut(usize, &[u8], Option<&Flds<N>>)) {
    let n = file_bytes(f).len();
    let lim = (e + 2048).min(n);
    if let Some(file) = file {
        // the bytes this block needs, copied into a small buffer that stays in
        // cache; positions below stay file offsets, shifted by `off`
        let off = s.saturating_sub(1) & !63;
        WINDOW.with(|w| {
            let mut win = w.borrow_mut();
            crate::read_range(file, off, lim, &mut win);
            scan_window::<N>(f, &win[..lim - off], off, n, s, e, lim, each);
        });
    } else {
        scan_window::<N>(f, file_bytes(f), 0, n, s, e, lim, each);
    }
}

/// `d` holds file bytes [off, off + d.len()), covering [s - 1, lim); `n` is the
/// file length. Positions are file offsets; byte x is d[x - off].
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn scan_window<const N: usize>(f: usize, d: &[u8], off: usize, n: usize, s: usize, e: usize, lim: usize, each: &mut impl FnMut(usize, &[u8], Option<&Flds<N>>)) {
    let at = |x: usize| d[x - off];
    let mut p = s;
    if s == 0 {
        p = match memchr(b'\n', &d[..lim - off]) {
            Some(i) => i + 1,
            None => lim,
        };
    } else if at(s - 1) != b'\n' {
        p = match memchr(b'\n', &d[s - off..lim - off]) {
            Some(i) => s + i + 1,
            None => lim,
        };
    }
    if p >= e {
        return;
    }
    let base = p & !63;
    let data = &d[..lim - off];
    SCANBUF.with(|cell| {
        let mut guard = cell.borrow_mut();
        let buf = &mut *guard;
        let words = stage1(&data[base - off..lim - off], p - base, lim == n, buf);
        let (nlb, bad, sep) = (&buf.nl[..], &buf.bad[..], &buf.sep[..]);
        let (nlp, badp, sepp, nsep) = (nlb.as_ptr(), bad.as_ptr(), sep.as_ptr(), sep.len());
        // first separator at or after p
        let mut j = sep.partition_point(|&x| (x as usize) < p - base);
        let mut w = (p - base) / 64;
        let mut cur = nlb[w] & (!0u64 << ((p - base) % 64));
        let mut fl = Flds::<N> { e: [0; N] };
        loop {
            while cur == 0 && w + 1 < words {
                w += 1;
                cur = unsafe { *nlp.add(w) };
            }
            let rel = p - base;
            let (end, ok) = if cur != 0 {
                let endr = w * 64 + cur.trailing_zeros() as usize;
                cur &= cur - 1;
                let mut ok = false;
                if j + N <= nsep && unsafe { *sepp.add(j + N - 1) } as usize == endr {
                    // violations are rare: three clean words cover lines of up to 129 bytes
                    let wa = rel / 64;
                    ok = if endr / 64 <= wa + 2 && unsafe { *badp.add(wa) | *badp.add(wa + 1) | *badp.add(wa + 2) } == 0 {
                        true
                    } else {
                        !any_bits(bad, rel, endr)
                    };
                    for i in 0..N {
                        fl.e[i] = unsafe { *sepp.add(j + i) } - rel as u32;
                    }
                    j += N;
                } else {
                    // malformed: skip this line's separators
                    while j < nsep && (unsafe { *sepp.add(j) } as usize) <= endr {
                        j += 1;
                    }
                }
                (base + endr, ok)
            } else {
                // no newline before the bitmap's end (a line over 2 KiB long):
                // the rare scalar path reads the mapping, then the block is done
                let (end, ok) = long_line::<N>(file_bytes(f), p, &mut fl);
                each(p, &file_bytes(f)[p..end], if ok { Some(&fl) } else { None });
                break;
            };
            let line = &data[p - off..end.min(n) - off];
            each(p, line, if ok { Some(&fl) } else { None });
            p = end + 1;
            if p >= e || w >= words {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests_support::{install, naive};

    fn run<const N: usize>(seed: u64, rounds: usize) {
        let _g = crate::tests_support::LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut x: u64 = seed ^ std::env::var("SS_TSEED").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let rounds = rounds * std::env::var("SS_TROUNDS").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1);
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alpha = b"ab,,,\"\"\n\r xy0123456789";
        for round in 0..rounds {
            let big = round + 40 >= rounds;
            let len = if big { 200_000 + (rnd() % 300_000) as usize } else { (rnd() % 3000) as usize + 1 };
            let mut d: Vec<u8> = b"h1,h2,h3,h4\n".to_vec();
            let quoting = rnd() % 3;
            for _ in 0..len {
                let r = rnd() % 100;
                let mut c = if r < 60 {
                    b"abcd"[(rnd() % 4) as usize]
                } else if r < 75 && quoting > 0 {
                    // whole-field quotes are common in real data
                    if rnd() % 2 == 0 { b',' } else { b'"' }
                } else {
                    alpha[(rnd() as usize) % alpha.len()]
                };
                if big && c == b'\n' && rnd() % 64 != 0 {
                    c = b'z';
                }
                d.push(c);
            }
            match round % 4 {
                0 => d.push(b'\n'),
                1 => {
                    // pad to a 64-byte boundary to hit the exact-boundary EOF case
                    while d.len() % 64 != 0 {
                        d.push(b'a');
                    }
                }
                _ => {}
            }
            install(&d);
            let want = naive::<N>(&d);
            let n = d.len();
            let mut cuts: Vec<usize> = (0..(rnd() % 6)).map(|_| (rnd() as usize) % n).collect();
            cuts.push(0);
            cuts.push(n);
            cuts.sort();
            cuts.dedup();
            let mut got = Vec::new();
            for w in cuts.windows(2) {
                scan_records::<N>(0, w[0], w[1], |p, line, fs| got.push((p, fs.map(|f| f.pairs(line)))));
            }
            let mut got_l = Vec::new();
            for w in cuts.windows(2) {
                scan_lines::<N>(0, w[0], w[1], |p, line, fs| got_l.push((p, fs.map(|f| f.pairs(line)))));
            }
            if got_l != want {
                for (i, (a, b)) in got_l.iter().zip(want.iter()).enumerate() {
                    if a != b {
                        let p = a.0.min(b.0);
                        let end = (p + 100).min(d.len());
                        panic!("scan_lines round {round} item {i}: got {a:?} want {b:?} line {:?} n={} cuts={cuts:?}", String::from_utf8_lossy(&d[p..end]), d.len());
                    }
                }
                panic!("scan_lines round {round}: lengths {} vs {}", got_l.len(), want.len());
            }
            if got != want {
                for (i, (a, b)) in got.iter().zip(want.iter()).enumerate() {
                    if a != b {
                        let p = a.0.min(b.0);
                        let end = (p + 80).min(d.len());
                        panic!("round {round} item {i}: got {a:?} want {b:?} line {:?} n={} cuts={cuts:?} prev={:?}", String::from_utf8_lossy(&d[p..end]), d.len(), String::from_utf8_lossy(&d[p.saturating_sub(70)..p]));
                    }
                }
                panic!("round {round}: lengths {} vs {}", got.len(), want.len());
            }
        }
    }

    /// Realistic lines (N fields of 0-24 bytes, often quoted, occasionally
    /// broken), lengths around the 64- and 96-byte edges.
    fn structured<const N: usize>(seed: u64, rounds: usize) {
        let _g = crate::tests_support::LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut x: u64 = seed ^ std::env::var("SS_TSEED").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..rounds {
            let mut d: Vec<u8> = b"h\n".to_vec();
            let lines = 1 + rnd() % 400;
            for _ in 0..lines {
                let nf = if rnd() % 20 == 0 { N + 1 - (rnd() % 3) as usize } else { N };
                for k in 0..nf {
                    if k > 0 {
                        d.push(b',');
                    }
                    let len = (rnd() % 25) as usize;
                    let quoted = rnd() % 3 == 0;
                    if quoted {
                        d.push(b'"');
                    }
                    for _ in 0..len {
                        let r = rnd() % 200;
                        d.push(if r == 0 { b'"' } else if r == 1 { b',' } else { b"abc0123xyz-:TZ"[(rnd() % 14) as usize] });
                    }
                    if quoted && rnd() % 30 != 0 {
                        d.push(b'"');
                    }
                }
                if rnd() % 50 == 0 {
                    d.push(b'\r');
                }
                d.push(b'\n');
            }
            if rnd() % 2 == 0 {
                d.pop();
            }
            install(&d);
            let want = naive::<N>(&d);
            let n = d.len();
            for (name, which) in [("records", 0), ("lines", 1)] {
                let mut got = Vec::new();
                let cut = (rnd() as usize) % n;
                for (a, b) in [(0, cut), (cut, n)] {
                    if which == 0 {
                        scan_records::<N>(0, a, b, |p, line, fs| got.push((p, fs.map(|f| f.pairs(line)))));
                    } else {
                        scan_lines::<N>(0, a, b, |p, line, fs| got.push((p, fs.map(|f| f.pairs(line)))));
                    }
                }
                if got != want {
                    for (i, (a, b)) in got.iter().zip(want.iter()).enumerate() {
                        if a != b {
                            let p = a.0.min(b.0);
                            let end = (p + 130).min(d.len());
                            panic!("{name} round {round} item {i}: got {a:?} want {b:?} line {:?}", String::from_utf8_lossy(&d[p..end]));
                        }
                    }
                    panic!("{name} round {round}: lengths {} vs {}", got.len(), want.len());
                }
            }
        }
    }

    #[test]
    fn scan_structured_7() {
        structured::<7>(0x7777_1111, 400);
    }

    #[test]
    fn scan_structured_4() {
        structured::<4>(0x4444_2222, 400);
    }

    #[test]
    fn scan_matches_scalar_4() {
        run::<4>(0x1234_5678, 3000);
    }

    #[test]
    fn scan_matches_scalar_7() {
        run::<7>(0x9876_5431, 2000);
    }

    #[test]
    fn scan_matches_scalar_2() {
        run::<2>(0x5555_1234, 2000);
    }
}
