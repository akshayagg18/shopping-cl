//! Exact 48-bit event identities and uncommon-value side tables.
//! Base62 symbols use six bits; 62 pads shorter strings and 63 escapes into
//! the exact byte dictionary. Thus leading zeros and letter case are preserved.
use std::{collections::HashMap,sync::Mutex};
const ESC:u64=63<<42;
const RARE_TIME:u64=ESC|(1<<41);
const WIDE_LOCATION:u64=1<<47;
#[derive(Default)]
pub struct Tokens {
    other:Mutex<HashMap<Vec<u8>,u64>>,
    times:Mutex<Vec<(u64,i64)>>,
    locations:Mutex<Vec<(usize,usize,usize)>>,
}

#[inline]
fn compact(bytes:&[u8])->Option<u64> {
    if !(2..=9).contains(&bytes.len())||bytes[0]!=b'e' {return None;}
    let len=bytes.len()-1;
    let mut padded=[b'0';8];
    let b=if len==8 {&bytes[1..]} else {padded[..len].copy_from_slice(&bytes[1..]);&padded[..]};
    #[cfg(all(target_arch="x86_64",target_feature="ssse3",target_feature="bmi2"))]
    let value=unsafe {
        use std::arch::x86_64::*;
        let v=_mm_loadl_epi64(b.as_ptr().cast());
        let in_range=|lo:i8,hi:i8|_mm_and_si128(_mm_cmpgt_epi8(v,_mm_set1_epi8(lo-1)),_mm_cmpgt_epi8(_mm_set1_epi8(hi+1),v));
        let digit=in_range(b'0' as i8,b'9' as i8);
        let upper=in_range(b'A' as i8,b'Z' as i8);
        let lower=in_range(b'a' as i8,b'z' as i8);
        if _mm_movemask_epi8(_mm_or_si128(digit,_mm_or_si128(upper,lower)))&255!=255 {return None;}
        let d=_mm_sub_epi8(_mm_sub_epi8(_mm_sub_epi8(v,_mm_set1_epi8(48)),_mm_and_si128(upper,_mm_set1_epi8(7))),_mm_and_si128(lower,_mm_set1_epi8(13)));
        let word=_mm_cvtsi128_si64(d) as u64;
        let keep=if len==8 {u64::MAX} else {(1u64<<(len*8))-1};
        _pext_u64((word&keep)|(0x3e3e_3e3e_3e3e_3e3e&!keep),0x3f3f_3f3f_3f3f_3f3f)
    };
    #[cfg(not(all(target_arch="x86_64",target_feature="ssse3",target_feature="bmi2")))]
    let value={
        let mut value=0u64;
        for i in 0..8 {
            let symbol=if i>=len {62} else {match b[i] {b'0'..=b'9'=>b[i]-b'0',b'A'..=b'Z'=>b[i]-b'A'+10,b'a'..=b'z'=>b[i]-b'a'+36,_=>return None}};
            value|=(symbol as u64)<<(6*i);
        }
        value
    };
    Some(value)
}

impl Tokens {
    #[inline]
    pub fn key(&self,bytes:&[u8])->u64 {
        if let Some(key)=compact(bytes) {return key;}
        crate::profile::lock_count(5);let mut other=self.other.lock().unwrap();
        if let Some(&key)=other.get(bytes) {return key;}
        let index=other.len() as u64;assert!(index<(1<<41));
        let key=ESC|index;other.insert(bytes.to_vec(),key);key
    }
    #[inline]
    pub fn stamp(&self,bytes:&[u8],seconds:i64)->(u64,u32,u64) {
        let key=self.key(bytes);
        if (0..=u32::MAX as i64).contains(&seconds) {return(key,seconds as u32,key);}
        crate::profile::lock_count(6);let mut times=self.times.lock().unwrap();let index=times.len() as u64;
        assert!(index<(1<<41));times.push((key,seconds));(RARE_TIME|index,0,key)
    }
    #[inline(always)]
    pub fn identity(&self,stored:u64)->u64 {
        if stored<RARE_TIME {stored} else {crate::profile::lock_count(7);self.times.lock().unwrap()[(stored&((1<<41)-1)) as usize].0}
    }
    #[inline(always)]
    pub fn seconds(&self,stored:u64,seconds:u32)->i64 {
        if stored<RARE_TIME {seconds as i64} else {crate::profile::lock_count(7);self.times.lock().unwrap()[(stored&((1<<41)-1)) as usize].1}
    }
    #[inline]
    pub fn location(&self,thread:usize,part:usize,index:usize)->u64 {
        if thread<128&&part<4096&&index<(1<<28)-1 {
            return ((thread as u64)<<40)|((part as u64)<<28)|(index as u64+1);
        }
        let mut loc=self.locations.lock().unwrap();let i=loc.len() as u64;
        assert!(i<WIDE_LOCATION);loc.push((thread,part,index));WIDE_LOCATION|i
    }
    #[inline]
    pub fn decode_location(&self,loc:u64)->(usize,usize,usize) {
        assert_ne!(loc,0);
        if loc&WIDE_LOCATION!=0 {self.locations.lock().unwrap()[(loc&(WIDE_LOCATION-1)) as usize]}
        else {((loc>>40) as usize,((loc>>28)&4095) as usize,((loc&((1<<28)-1))-1) as usize)}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn symbols_lengths_and_fallback_are_exact() {
        let tokens=Tokens::default();let mut seen=std::collections::HashSet::new();
        let alphabet=b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for len in 1..=8 {
            for &ch in alphabet {
                let mut b=vec![b'e'];b.extend(std::iter::repeat_n(ch,len));
                let key=tokens.key(&b);assert!(seen.insert(key));assert_eq!(key,tokens.key(&b));
            }
        }
        for b in [b"".as_slice(),b"e",b"E00000000",b"exxxxxxxxz",b"e\r",b"e{",b"e\xc3\xa9",b"other-long-label"] {
            let key=tokens.key(b);assert!(seen.insert(key));assert_eq!(key,tokens.key(b));
        }
        let mut seed=1u64;
        for _ in 0..100_000 {
            seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;
            let mut b=vec![b'e'];let n=1+seed as usize%8;
            for i in 0..n {b.push(alphabet[(seed>>(i*7)) as usize%alphabet.len()]);}
            let key=tokens.key(&b);
            let mut decoded=vec![b'e'];for i in 0..8 {let c=((key>>(6*i))&63) as usize;if c==62{break;}decoded.push(alphabet[c]);}
            assert_eq!(b,decoded);
        }
    }
    #[test]
    fn full_time_and_wide_locations_round_trip() {
        let tokens=Tokens::default();let identity=tokens.key(b"e00000000");
        for sec in [i64::MIN/1000,-2208988800,-1,0,1,u32::MAX as i64,u32::MAX as i64+1,i64::MAX/1000] {
            let (stored,compact,key)=tokens.stamp(b"e00000000",sec);
            assert_eq!(key,identity);assert_eq!(tokens.identity(stored),identity);assert_eq!(tokens.seconds(stored,compact),sec);
        }
        for (t,p,i) in [(0,0,0),(7,2047,60_000_000),(127,4095,(1<<28)-2),(128,0,0),(1,16383,1),(1,0,1<<32)] {
            let encoded=tokens.location(t,p,i);assert_ne!(encoded,0);assert_eq!(tokens.decode_location(encoded),(t,p,i));
        }
    }
}
