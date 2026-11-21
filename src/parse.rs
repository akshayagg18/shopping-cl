//! Byte-level CSV dialect and timestamp parsing, exactly as rules.md defines them.

pub const DAY_MS: i64 = 86_400_000;

/// Split one physical line (no trailing '\n') into exactly N fields, unquoting
/// whole-field quotes. Returns None for a malformed line.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8]) -> Option<[(u32, u32); N]> {
    let mut out = [(0u32, 0u32); N];
    let mut k = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32;
    let mut first_q = usize::MAX;
    let mut last_q = 0usize;
    let len = line.len();
    let mut i = 0usize;
    loop {
        let at_end = i == len;
        let b = if at_end { b',' } else { line[i] };
        if b == b',' {
            if k == N {
                return None;
            }
            let (mut s, mut e) = (start, i);
            if quotes != 0 {
                if quotes != 2 || first_q != s || last_q + 1 != e || e - s < 2 {
                    return None;
                }
                s += 1;
                e -= 1;
            }
            out[k] = (s as u32, e as u32);
            k += 1;
            if at_end {
                break;
            }
            start = i + 1;
            quotes = 0;
            first_q = usize::MAX;
        } else if b == b'"' {
            if quotes == 0 {
                first_q = i;
            }
            quotes += 1;
            last_q = i;
        }
        i += 1;
    }
    if k != N {
        return None;
    }
    Some(out)
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        return None;
    }
    Some((a as i64) * 10 + c as i64)
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

const Y0: i64 = 1900;
const YN: usize = 400;

/// Days since epoch at the first of each month for years Y0..Y0+YN, and month lengths.
struct Cal {
    month_start: Vec<i32>,
    month_len: Vec<u8>,
}

static CAL: std::sync::LazyLock<Cal> = std::sync::LazyLock::new(|| {
    let mut month_start = Vec::with_capacity(YN * 16);
    let mut month_len = Vec::with_capacity(YN * 16);
    for y in 0..YN as i64 {
        for m in 0..16i64 {
            if (1..=12).contains(&m) {
                month_start.push(days_from_civil(Y0 + y, m, 1) as i32);
                month_len.push(days_in_month(Y0 + y, m) as u8);
            } else {
                month_start.push(0);
                month_len.push(0); // month 0, 13..15: no valid day
            }
        }
    }
    Cal { month_start, month_len }
});

/// F1/F2 calendar part `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) -> epoch ms (UTC, no offset).
#[inline(always)]
fn civil19(b: &[u8]) -> Option<i64> {
    let g = |i: usize| unsafe { b.get_unchecked(i).wrapping_sub(b'0') } as u32;
    let d = [g(0), g(1), g(2), g(3), g(5), g(6), g(8), g(9), g(11), g(12), g(14), g(15), g(17), g(18)];
    let mut bad = 0u32;
    for x in d {
        bad |= (x > 9) as u32;
    }
    bad |= (b[4] != b'-') as u32 | (b[7] != b'-') as u32 | (b[10] != b'T') as u32;
    bad |= (b[13] != b':') as u32 | (b[16] != b':') as u32;
    let y = (d[0] * 1000 + d[1] * 100 + d[2] * 10 + d[3]) as i64;
    let mo = (d[4] * 10 + d[5]) as usize;
    let day = d[6] * 10 + d[7];
    let h = d[8] * 10 + d[9];
    let mi = d[10] * 10 + d[11];
    let sec = d[12] * 10 + d[13];
    bad |= (h > 23) as u32 | (mi > 59) as u32 | (sec > 59) as u32 | (mo > 15) as u32 | (day == 0) as u32;
    if bad != 0 {
        return None;
    }
    let days = if (Y0..Y0 + YN as i64).contains(&y) {
        let i = (y - Y0) as usize * 16 + mo;
        let cal = &*CAL;
        if day > cal.month_len[i] as u32 {
            return None;
        }
        cal.month_start[i] as i64 + day as i64 - 1
    } else {
        if !(1..=12).contains(&(mo as i64)) || day as i64 > days_in_month(y, mo as i64) {
            return None;
        }
        days_from_civil(y, mo as i64, day as i64)
    };
    Some((days * 86_400 + (h * 3_600 + mi * 60 + sec) as i64) * 1_000)
}

/// Any of F1, F2, F3 -> epoch ms. None for a bad timestamp (window not checked).
#[inline]
pub fn timestamp(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return civil19(b);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = civil19(b)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    // F3: digits only. Fast path for 9..=19 digits: a short scalar prefix,
    // then the last 8 digits in one SWAR step.
    if (9..=19).contains(&n) {
        let mut hi: u64 = 0;
        let mut bad = 0u8;
        for &c in &b[..n - 8] {
            let d = c.wrapping_sub(b'0');
            bad |= (d > 9) as u8;
            hi = hi * 10 + d as u64;
        }
        let w = u64::from_le_bytes(b[n - 8..].try_into().unwrap());
        let x = w.wrapping_sub(0x3030_3030_3030_3030);
        // every byte must be 0..=9: no borrow and no byte above 9
        let over = w.wrapping_add(0x4646_4646_4646_4646);
        if bad != 0 || ((x | over) & 0x8080_8080_8080_8080) != 0 {
            return None;
        }
        let lo = eight_digits(x);
        // n - 8 <= 11 digits in `hi`, so hi * 1e8 + lo fits in u64 (< 1e19)
        let v = hi * 100_000_000 + lo;
        return Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 });
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = match v.checked_mul(10).and_then(|x| x.checked_add(d as u64)) {
            Some(x) => x,
            // Too long for 64 bits: a valid form, but outside any window.
            None => return Some(i64::MAX),
        };
    }
    Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
}

/// Eight digit values (one per byte, first digit lowest) -> number (fast_float's trick).
#[inline(always)]
fn eight_digits(x: u64) -> u64 {
    let x = (x.wrapping_mul(10)).wrapping_add(x >> 8);
    let a = (x & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32));
    let b = ((x >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32));
    (a.wrapping_add(b)) >> 32
}

/// purchase price: ASCII digits only, value 1..=10_000_000.
#[inline]
pub fn price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning for non-digits is unnecessary: already invalid
            return None;
        }
    }
    if v == 0 { None } else { Some(v as u32) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ts_forms() {
        assert_eq!(timestamp(b"2026-10-03T23:58:10Z"), timestamp(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(timestamp(b"1791071890000"), Some(1791071890000));
        assert_eq!(timestamp(b"0001791071890000"), Some(1791071890000));
        assert_eq!(timestamp(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert!(timestamp(b"2026-02-29T00:00:00Z").is_none());
        assert!(timestamp(b"2028-02-29T00:00:00Z").is_some());
        assert!(timestamp(b"2026-10-03T23:58:60Z").is_none());
        assert!(timestamp(b"2026-10-03T24:58:00Z").is_none());
        assert!(timestamp(b"2026-10-03T10:00:00+24:00").is_none());
        assert!(timestamp(b"2026-10-03T10:00:00+05:60").is_none());
        assert!(timestamp(b"2026-10-03 10:00:00Z").is_none());
        assert!(timestamp(b"1.79e12").is_none());
        assert!(timestamp(b"-5").is_none());
        assert_eq!(timestamp(b"179102160000000000000"), Some(i64::MAX));
        assert_eq!(timestamp(b"123456789"), Some(123456789));
        assert_eq!(timestamp(b"9999999999999999999"), Some(i64::MAX));
        assert_eq!(timestamp(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(timestamp(b"0000000000000000001"), Some(1));
        assert_eq!(timestamp(b"1791071890000.0"), None);
        assert_eq!(timestamp(b"17910718900:0"), None);
        assert_eq!(timestamp(b"1791071/90000"), None);
        for n in 1..=19u32 {
            let v = 10u64.pow(n - 1) + 7;
            let s = v.to_string();
            assert_eq!(timestamp(s.as_bytes()), Some(v.min(i64::MAX as u64) as i64), "{s}");
        }
    }
    #[test]
    fn splitting() {
        let l = b"a,\"b\",,\"\"";
        let f = split::<4>(l).unwrap();
        assert_eq!(&l[f[1].0 as usize..f[1].1 as usize], b"b");
        assert_eq!(f[2].0, f[2].1);
        assert_eq!(f[3].0, f[3].1);
        assert!(split::<4>(b"a,b,c").is_none());
        assert!(split::<2>(b"u1,u5\"").is_none());
        assert!(split::<2>(b"\"u1,u5").is_none());
        assert!(split::<2>(b"").is_none());
        assert!(split::<1>(b"").is_some());
        assert!(split::<2>(b"a\"b\",c").is_none());
    }
}
