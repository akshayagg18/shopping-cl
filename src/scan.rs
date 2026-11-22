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
        let q = (b > a && unsafe { *line.get_unchecked(a) } == b'"') as usize;
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
fn stage1(d: &[u8], first: usize, eof: bool, buf: &mut ScanBuf) -> usize {
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
            let (nl, cm, q) = masks64(std::slice::from_raw_parts(dp.add(w * 64), 64));
            s1_word(&mut st, nl, cm, q & qmask, w, nlo, bado, sepo);
            qmask = !0;
        }
        if words > full {
            let mut tail = [0u8; 64];
            let rest = &d[full * 64..];
            tail[..rest.len()].copy_from_slice(rest);
            let (nl, cm, q) = masks64(&tail);
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

/// Iterate records starting in [s, e) (a file's header skipped), split into N
/// fields: `each(line_start, line, fields)`, fields None for a malformed line.
#[inline(always)]
pub fn scan_records<const N: usize>(f: usize, s: usize, e: usize, mut each: impl FnMut(usize, &'static [u8], Option<&Flds<N>>)) {
    const SUB: usize = 192 << 10;
    let mut a = s;
    while a < e {
        let b = (a + SUB).min(e);
        scan_block::<N>(f, a, b, &mut each);
        a = b;
    }
}

#[inline(always)]
fn scan_block<const N: usize>(f: usize, s: usize, e: usize, each: &mut impl FnMut(usize, &'static [u8], Option<&Flds<N>>)) {
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
    let base = p & !63;
    let lim = (e + 2048).min(n);
    SCANBUF.with(|cell| {
        let mut guard = cell.borrow_mut();
        let buf = &mut *guard;
        let words = stage1(&data[base..lim], p - base, lim == n, buf);
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
                // no newline before the bitmap's end (a line over 2 KiB long)
                let (end, ok) = long_line::<N>(data, p, &mut fl);
                // past the bitmap: this is the last line of the block
                cur = 0;
                w = words;
                (end, ok)
            };
            let line = &data[p..end.min(n)];
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
