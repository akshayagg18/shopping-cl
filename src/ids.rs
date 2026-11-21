//! Injective ID values: reversible short ASCII words and exact byte interning.
//! The permutation spreads the keys without discarding identifier information.
use std::{collections::HashMap, sync::{LazyLock, Mutex}};
const INTERNED: u64 = 1 << 62;
#[derive(Default)]
struct Other { map:HashMap<&'static [u8],u64>, names:Vec<&'static [u8]> }
// These bytes live for the process, just like the input mappings. Only IDs that
// cannot be represented reversibly use this dictionary.
static OTHER: LazyLock<Mutex<Other>> = LazyLock::new(|| Mutex::new(Other::default()));

#[inline(always)]
fn payload(b: &[u8]) -> u64 {
    let n=b.len();
    let short=if n==9 && matches!(b[0],b'u'|b'U'|b'e'|b'E') {
        let tag=match b[0] {b'u'=>9,b'U'=>10,b'e'=>11,_=>12};
        Some((u64::from_le_bytes(b[1..].try_into().unwrap()),tag))
    } else if (1..=8).contains(&n) {
        let mut buf=[0u8;8];buf[..n].copy_from_slice(b);Some((u64::from_le_bytes(buf),n as u64))
    } else {None};
    if let Some((word,tag))=short {
        if word&0x8080_8080_8080_8080==0 {
            #[cfg(all(target_arch="x86_64",target_feature="bmi2"))]
            let packed=unsafe {std::arch::x86_64::_pext_u64(word,0x7f7f_7f7f_7f7f_7f7f)};
            #[cfg(not(all(target_arch="x86_64",target_feature="bmi2")))]
            let packed=(0..8).fold(0u64,|v,i|v|(((word>>(i*8))&127)<<(i*7)));
            return packed<<4|tag;
        }
    }
    let mut map=OTHER.lock().unwrap();
    if let Some(&key)=map.map.get(b) {return key;}
    let index=map.names.len() as u64;assert!(index<INTERNED);
    let key=INTERNED|index;
    let bytes:&'static [u8]=Box::leak(b.to_vec().into_boxed_slice());
    map.map.insert(bytes,key);map.names.push(bytes);key
}

#[inline(always)]
fn mix(mut x:u64)->u64 {
    x^=x>>30;x=x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x^=x>>27;x=x.wrapping_mul(0x94d0_49bb_1331_11eb);x^(x>>31)
}
#[inline(always)]
pub fn unmix(mut x:u64)->u64 {
    x^=(x>>31)^(x>>62);x=x.wrapping_mul(0x3196_42b2_d24d_8ec3);
    x^=(x>>27)^(x>>54);x=x.wrapping_mul(0x96de_1b17_3f11_9089);
    x^(x>>30)^(x>>60)
}
#[inline(always)]
pub fn key(b:&[u8])->u64 {mix(payload(b))}

/// Decode an unlinked person's injective payload for output.
pub fn bytes<'a>(key:u64,buffer:&'a mut [u8;9])->&'a [u8] {
    if key&INTERNED!=0 {return OTHER.lock().unwrap().names[(key&!INTERNED) as usize];}
    let tag=(key&15) as usize;
    let packed=key>>4;
    #[cfg(all(target_arch="x86_64",target_feature="bmi2"))]
    let word=unsafe {std::arch::x86_64::_pdep_u64(packed,0x7f7f_7f7f_7f7f_7f7f)};
    #[cfg(not(all(target_arch="x86_64",target_feature="bmi2")))]
    let word=(0..8).fold(0u64,|v,i|v|(((packed>>(i*7))&127)<<(i*8)));
    if tag<=8 {buffer[..8].copy_from_slice(&word.to_le_bytes());&buffer[..tag]}
    else {
        buffer[0]=match tag {9=>b'u',10=>b'U',11=>b'e',12=>b'E',_=>unreachable!()};
        buffer[1..].copy_from_slice(&word.to_le_bytes());&buffer[..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permutation_and_domains() {
        let mut x=123456u64;
        for _ in 0..100_000 {
            x^=x<<13;x^=x>>7;x^=x<<17;
            assert_eq!(unmix(mix(x)),x);
        }
        let values:Vec<&[u8]>=vec![b"",b"\0",b"\0\0",b"a",b"a\0",b"u12345678",b"U12345678",b"e12345678",b"E12345678",b"12345678",b"arbitrary-long-user",b"\xc3\xa9"];
        let keys:std::collections::HashSet<_>=values.iter().map(|v|key(v)).collect();
        assert_eq!(keys.len(),values.len());
        for v in values {assert_eq!(unmix(key(v))>>63,0);assert_ne!(key(v),0);assert_eq!(bytes(unmix(key(v)), &mut [0;9]), v);}
    }
}
