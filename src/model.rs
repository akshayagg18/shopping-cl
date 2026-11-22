//! Reversible, runtime-keyed ID layout. Dense placement is only an optimization:
//! every unrecognized spelling or out-of-range value keeps the exact general key.
use rayon::prelude::*;
const MASK:u64=(1<<40)-1;
const FIRST:u64=0x3779_b97f4b;
const SECOND:u64=0xe8fe_b86659;
const FIRST_INV:u64=0x3dee_219263;
const SECOND_INV:u64=0xa090_5881e9;

#[derive(Clone,Copy,Debug)]
pub struct Model {pub prefix:u8,pub key:u64,pub bound:u32}

/// Canonical base36 in the public generator's 40-bit domain. Both spelling
/// and case are checked, so decoding is injective on the accepted byte strings.
#[inline]
pub fn number(bytes:&[u8],prefix:u8)->Option<u64> {
    let n=bytes.len();
    if !(2..=9).contains(&n)||bytes[0]!=prefix||(n>2&&bytes[1]==b'0') {return None;}
    let len=n-1;let mut padded=[b'0';8];
    let digits=if len==8 {&bytes[1..]} else {padded[8-len..].copy_from_slice(&bytes[1..]);&padded[..]};
    #[cfg(all(target_arch="x86_64",target_feature="ssse3"))]
    let value=unsafe {
        use std::arch::x86_64::*;
        let v=_mm_loadl_epi64(digits.as_ptr().cast());
        let d=_mm_sub_epi8(v,_mm_set1_epi8(b'0' as i8));
        let a=_mm_sub_epi8(v,_mm_set1_epi8(b'a' as i8));
        let is_d=_mm_cmpeq_epi8(_mm_min_epu8(d,_mm_set1_epi8(9)),d);
        let is_a=_mm_cmpeq_epi8(_mm_min_epu8(a,_mm_set1_epi8(25)),a);
        if _mm_movemask_epi8(_mm_or_si128(is_d,is_a))&255!=255 {return None;}
        let values=_mm_sub_epi8(d,_mm_and_si128(is_a,_mm_set1_epi8(39)));
        let pairs=_mm_maddubs_epi16(values,_mm_set1_epi16(0x0124));
        let quads=_mm_madd_epi16(pairs,_mm_set1_epi32(0x0001_0510));
        let word=_mm_cvtsi128_si64(quads) as u64;
        (word as u32 as u64)*1_679_616+(word>>32)
    };
    #[cfg(not(all(target_arch="x86_64",target_feature="ssse3")))]
    let value={
        let mut value=0;
        for &c in digits {value=value*36+match c {b'0'..=b'9'=>(c-b'0') as u64,b'a'..=b'z'=>(c-b'a'+10) as u64,_=>return None};}
        value
    };
    (value<=MASK).then_some(value)
}
#[inline(always)]
fn undo_second(value:u64)->u64 {
    let x=value.wrapping_mul(SECOND_INV)&MASK;
    x^(x>>17)^(x>>34)
}
impl Model {
    #[inline(always)]
    pub fn decode_value(&self,value:u64)->u64 {(undo_second(value)^self.key).wrapping_mul(FIRST_INV)&MASK}
    #[inline]
    pub fn decode(&self,bytes:&[u8])->Option<u64> {number(bytes,self.prefix).map(|v|self.decode_value(v))}
    #[inline]
    pub fn event(&self,bytes:&[u8])->Option<u32> {
        let value=self.decode(bytes)?;(value<self.bound as u64).then_some(value as u32)
    }
    /// Three low alias bits are compressed away from the unused 512-wide gap.
    /// Every other alias remains in the general exact dictionary.
    #[inline]
    pub fn user(&self,bytes:&[u8])->Option<u32> {
        let value=self.decode(bytes)?;let group=value>>9;let alias=value&511;
        (group<self.bound as u64&&alias<8).then_some(((group<<3)|alias) as u32)
    }
    pub fn label(&self,index:u64,out:&mut [u8;10])->usize {
        let mut value=(index.wrapping_mul(FIRST)^self.key)&MASK;
        value^=value>>17;value=value.wrapping_mul(SECOND)&MASK;
        let mut reverse=[0u8;8];let mut n=0;
        loop {reverse[n]=b"0123456789abcdefghijklmnopqrstuvwxyz"[(value%36) as usize];n+=1;value/=36;if value==0 {break;}}
        out[0]=self.prefix;for i in 0..n {out[1+i]=reverse[n-i-1];}n+1
    }
}

fn samples(files:&[usize],column:usize,prefix:u8)->Vec<u64> {
    let mut result=Vec::new();
    for &file in files {
        let data=crate::file_bytes(file);let mut p=crate::memchr(b'\n',data).map_or(data.len(),|i|i+1);
        let mut examined=0;
        while p<data.len()&&examined<128&&result.len()<12 {
            let end=crate::memchr(b'\n',&data[p..]).map_or(data.len(),|i|p+i);
            if let Some(fs)=crate::parse::split::<7>(&data[p..end]) {
                let (a,b)=fs.get(column);
                if let Some(value)=number(&data[p+a..p+b],prefix) {if !result.contains(&value) {result.push(value);}}
            }
            examined+=1;p=end+1;
        }
        if result.len()>=12 {break;}
    }
    result
}

/// Sample different byte regions, rather than only the first whale's aliases.
fn distributed_users(files:&[usize])->Vec<u64> {
    let mut ordered=files.to_vec();ordered.sort_unstable_by_key(|&f|std::cmp::Reverse(crate::file_bytes(f).len()));
    let mut values=Vec::new();
    for &file in &ordered {
        let data=crate::file_bytes(file);
        for fraction in 0..8 {
            let offset=data.len()/8*fraction;
            let mut at=crate::memchr(b'\n',&data[offset..]).map_or(data.len(),|i|offset+i+1);
            let mut added=0;
            for _ in 0..128 {
                if at>=data.len()||values.len()>=64||added>=8 {break;}
                let end=crate::memchr(b'\n',&data[at..]).map_or(data.len(),|i|at+i);
                if let Some(fields)=crate::parse::split::<7>(&data[at..end]) {
                    let(a,b)=fields.get(1);
                    if let Some(value)=number(&data[at+a..at+b],b'u') {if !values.contains(&value) {values.push(value);added+=1;}}
                }
                at=end+1;
            }
        }
        if values.len()>=64 {break;}
    }
    values
}

/// Search affects density, never correctness. Any selected key is a bijection;
/// labels outside the chosen range take an exact fallback.
pub fn learn_events(files:&[usize],bytes:usize)->Option<Model> {
    let s=samples(files,0,b'e');if s.len()<8 {return None;}
    let zs:Vec<_>=s.iter().map(|&v|undo_second(v)).collect();
    let bound=(bytes/64+4096).clamp(1,(1<<30)-1) as u32;
    let key=(0..bound).into_par_iter().find_map_any(|n| {
        let key=zs[0]^((n as u64).wrapping_mul(FIRST)&MASK);
        let required=s.len()*3/4;let mut fits=1;let mut misses=0;
        for &z in &zs[1..] {
            if ((z^key).wrapping_mul(FIRST_INV)&MASK)<bound as u64 {fits+=1;} else {misses+=1;}
            if misses>s.len()-required {return None;}
        }
        (fits>=required).then_some(key)
    })?;
    Some(Model{prefix:b'e',key,bound})
}
pub fn learn_users(files:&[usize],bytes:usize)->Option<Model> {
    let scale=std::env::var("SS_USER_BOUND_SCALE").ok().and_then(|v|v.parse::<usize>().ok()).unwrap_or(1).clamp(1,16);
    let learned=learn_users_mode(files,bytes.saturating_mul(scale),std::env::var("SS_STRATIFY_USER").is_ok_and(|v|v!="0"),std::env::var("SS_USER_FIRST").is_ok_and(|v|v!="0"));
    if std::env::var_os("SS_TIMING").is_some() {
        if let Some(model)=learned {let probe=distributed_users(files);let n=probe.iter().filter(|&&v|{let n=model.decode_value(v);(n>>9)<model.bound as u64&&(n&511)<8}).count();eprintln!("dense model independent common probe: {n}/{}",probe.len());}
    }
    learned
}
fn learn_users_mode(files:&[usize],bytes:usize,distributed:bool,ordered:bool)->Option<Model> {
    let s=if distributed {distributed_users(files)} else {samples(files,1,b'u')};if s.len()<8 {return None;}
    let zs:Vec<_>=s.iter().map(|&v|undo_second(v)).collect();
    let bound=(bytes/75/24+1024).clamp(1,1<<23) as u32;
    let domain=(bound as u64)*8;
    for anchor in 0..s.len().min(3) {
        let candidate=|i:u64| {
            let index=((i>>3)<<9)|(i&7);
            let key=zs[anchor]^(index.wrapping_mul(FIRST)&MASK);
            let required=if distributed {s.len()*9/10} else {s.len()*3/4};let mut fits=0;let mut misses=0;
            for &z in &zs {
                let n=(z^key).wrapping_mul(FIRST_INV)&MASK;
                if (n>>9)<bound as u64&&(n&511)<8 {fits+=1;} else {misses+=1;}
                if misses>s.len()-required {return None;}
            }
            (fits>=required).then_some(key)
        };
        let key=if ordered {(0..domain as usize).into_par_iter().find_map_first(|i|candidate(i as u64))} else {(0..domain).into_par_iter().find_map_any(candidate)};
        if let Some(key)=key {return Some(Model{prefix:b'u',key,bound});}
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reversible_labels_and_exact_spellings() {
        let mut seed=19u64;
        for key in [0,1,0x0123_4567_89,0xffff_ffff_ff] {
            let model=Model{prefix:b'e',key,bound:10000};
            for _ in 0..100000 {
                seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;let index=seed&MASK;
                let mut b=[0;10];let n=model.label(index,&mut b);
                assert_eq!(model.decode(&b[..n]),Some(index));
                let numeric=std::str::from_utf8(&b[1..n]).unwrap();assert_eq!(number(&b[..n],b'e'),u64::from_str_radix(numeric,36).ok());
            }
        }
        for b in [b"e00".as_slice(),b"eA",b"e+1",b"e-1",b"e1\r",b"e",b"",b"E1",b"e\xc3\xa9"] {assert_eq!(number(b,b'e'),None);}
        assert_ne!(number(b"e0",b'e'),None);
        assert_eq!(FIRST.wrapping_mul(FIRST_INV)&MASK,1);assert_eq!(SECOND.wrapping_mul(SECOND_INV)&MASK,1);
    }
    #[test]
    fn distributed_learning_covers_users_beyond_the_first_whale() {
        let _guard=crate::tests_support::LOCK.lock().unwrap_or_else(|e|e.into_inner());
        let model=Model{prefix:b'u',key:0x98127346,bound:4096};let mut data=b"h,h,h,h,h,h,h\n".to_vec();let mut labels=Vec::new();
        for group in 0..128 {for alias in 0..8 {let mut out=[0;10];let n=model.label((group<<9)|alias,&mut out);labels.push(out[..n].to_vec());}}
        for row in 0..50000 {
            let index=if row<20000 {row%6} else {row%labels.len()};
            data.extend_from_slice(b"e0,");data.extend_from_slice(&labels[index]);data.extend_from_slice(b",2026-10-03T00:00:00Z,2026-10-03T00:00:00Z,view,p1,\n");
        }
        crate::tests_support::install(&data);
        let mut chosen=None;
        for threads in [1,2,4] {
            let pool=rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
            let learned=pool.install(||learn_users_mode(&[0],data.len(),true,true)).unwrap();
            for label in &labels {assert!(learned.user(label).is_some(),"{:?}",label);}
            if let Some(key)=chosen {assert_eq!(learned.key,key);}chosen=Some(learned.key);
        }
    }
}
