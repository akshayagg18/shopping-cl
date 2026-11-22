//! Exact 48-bit event identities and uncommon-value side tables.
//! Base62 symbols use six bits; 62 pads shorter strings and 63 escapes into
//! the exact byte dictionary. Thus leading zeros and letter case are preserved.
use std::{collections::HashMap,sync::Mutex};
const ESC:u64=63<<42;
const RARE_TIME:u64=ESC|(1<<41);
const WIDE_LOCATION:u64=1<<47;
const SHARD_BITS:usize=6;
const SHARDS:usize=1<<SHARD_BITS;
#[repr(align(64))]
struct EventShard(Mutex<Other>);
#[derive(Default)]
struct Other { map:HashMap<&'static [u8],u64>, names:Vec<OwnedName> }
#[derive(Clone,Copy)]
struct OwnedName { bytes:&'static [u8], hash:u64 }
pub struct Tokens {
    other:Vec<EventShard>,
    shard_mask:usize,
    refs:bool,
    base:usize,
    end:usize,
    #[cfg(test)]
    forced_hash:Option<u64>,
    times:Mutex<Vec<(u64,i64)>>,
    locations:Mutex<Vec<(usize,usize,usize)>>,
}
impl Default for Tokens {
    fn default()->Self {
        let count=std::env::var("SS_EVENT_SHARDS").ok().and_then(|s|s.parse::<usize>().ok()).unwrap_or(SHARDS).clamp(1,SHARDS).next_power_of_two();
        Self{other:(0..SHARDS).map(|_|EventShard(Mutex::new(Other::default()))).collect(),shard_mask:count-1,refs:std::env::var("SS_EVENT_REFS").map_or(true,|s|s=="1"),base:0,end:0,#[cfg(test)] forced_hash:None,times:Mutex::new(Vec::new()),locations:Mutex::new(Vec::new())}
    }
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
    pub fn with_files(files:&[usize])->Self {
        let mut tokens=Self::default();
        if tokens.refs {
            let ranges:Vec<_>=files.iter().map(|&f|crate::file_bytes(f)).collect();
            tokens.set_ranges(&ranges);
        }
        tokens
    }
    fn set_ranges(&mut self,ranges:&[&'static [u8]]) {
        let base=ranges.iter().filter(|b|!b.is_empty()).map(|b|b.as_ptr().expose_provenance()).min().unwrap_or(0);
        let end=ranges.iter().map(|b|b.as_ptr().expose_provenance().checked_add(b.len()).unwrap()).max().unwrap_or(0);
        if base!=0 && end>=base && end-base<=1usize<<35 {self.base=base;self.end=end;}
    }
    #[cfg(test)]
    pub(crate) fn test_refs(bytes:&'static [u8],forced_hash:Option<u64>)->Self {
        let mut tokens=Self::default();tokens.refs=true;tokens.set_ranges(&[bytes]);tokens.forced_hash=forced_hash;tokens
    }
    #[inline]
    pub fn key(&self,bytes:&[u8])->u64 {
        if let Some(key)=compact(bytes) {return key;}
        let shard=if self.shard_mask==0 {0} else {xxhash_rust::xxh3::xxh3_64(bytes) as usize&self.shard_mask};
        let mut other=self.other[shard].0.lock().unwrap();
        if let Some(&key)=other.map.get(bytes) {return key;}
        let local=other.names.len() as u64;
        assert!(local<(1<<(41-SHARD_BITS-if self.refs {6} else {0})));
        let index=(local<<SHARD_BITS)|shard as u64;
        // Reference mode reserves low six bits for a source-field length.
        // Zero denotes an owned fallback, with an exact reverse dictionary.
        let key=ESC|if self.refs {index<<6} else {index};
        let bytes:&'static [u8]=Box::leak(bytes.to_vec().into_boxed_slice());
        other.map.insert(bytes,key);other.names.push(OwnedName{bytes,hash:xxhash_rust::xxh3::xxh3_64(bytes)});key
    }
    #[inline]
    fn source_key(&self,bytes:&'static [u8])->u64 {
        if let Some(key)=compact(bytes) {return key;}
        let address=bytes.as_ptr().expose_provenance();
        if self.refs && self.base!=0 && (1..=63).contains(&bytes.len()) && address>=self.base && address<=self.end && bytes.len()<=self.end-address {
            return ESC|(((address-self.base) as u64)<<6)|bytes.len() as u64;
        }
        self.key(bytes)
    }
    #[inline]
    fn raw_identity(&self,stored:u64)->u64 {
        if stored<RARE_TIME {stored} else {self.times.lock().unwrap()[(stored&((1<<41)-1)) as usize].0}
    }
    #[inline]
    fn bytes(&self,key:u64)->&'static [u8] {
        debug_assert!(self.refs && key>=ESC && key<RARE_TIME);
        let len=(key&63) as usize;
        if len!=0 {
            let offset=((key&((1<<41)-1))>>6) as usize;
            // Only source_key creates these tokens from live static slices.
            // It exposes their provenance and bounds-checks the entire field.
            // Input mappings are retained through dedup; no token is read after
            // their release. The encoding preserves every address/length bit.
            let ptr=std::ptr::with_exposed_provenance::<u8>(self.base+offset);
            return unsafe {std::slice::from_raw_parts(ptr,len)};
        }
        let index=(key&((1<<41)-1))>>6;
        self.other[index as usize&(SHARDS-1)].0.lock().unwrap().names[(index>>SHARD_BITS) as usize].bytes
    }
    #[inline]
    pub fn stamp(&self,bytes:&'static [u8],seconds:i64)->(u64,u32,u64) {
        let key=self.source_key(bytes);
        let hash=self.identity(key);
        if (0..=u32::MAX as i64).contains(&seconds) {return(key,seconds as u32,hash);}
        let mut times=self.times.lock().unwrap();let index=times.len() as u64;
        assert!(index<(1<<41));times.push((key,seconds));(RARE_TIME|index,0,hash)
    }
    #[inline(always)]
    pub fn identity(&self,stored:u64)->u64 {
        #[cfg(test)] if let Some(hash)=self.forced_hash {return hash;}
        let key=self.raw_identity(stored);
        if !self.refs || key<ESC {return key;}
        if key&63!=0 {return xxhash_rust::xxh3::xxh3_64(self.bytes(key));}
        let index=(key&((1<<41)-1))>>6;
        self.other[index as usize&(SHARDS-1)].0.lock().unwrap().names[(index>>SHARD_BITS) as usize].hash
    }
    /// Hashes select buckets only. Full bytes decide equality in reference mode.
    #[inline]
    pub fn same_id(&self,a:u64,b:u64)->bool {
        let a=self.raw_identity(a);let b=self.raw_identity(b);
        if a==b {return true;}
        if !self.refs || a<ESC || b<ESC {return false;}
        self.bytes(a)==self.bytes(b)
    }
    /// Hint only: uncommon time wrappers and owned entries are skipped.
    #[inline]
    pub fn prefetch(&self,stored:u64) {
        #[cfg(target_arch="x86_64")]
        if self.refs && stored>=ESC && stored<RARE_TIME && stored&63!=0 {
            let len=(stored&63) as usize;
            let offset=((stored&((1<<41)-1))>>6) as usize;
            let ptr=std::ptr::with_exposed_provenance::<i8>(self.base+offset);
            // Same source-field bounds and lifetime as bytes(); never read or
            // write through this pointer, and never step beyond its last byte.
            unsafe {
                std::arch::x86_64::_mm_prefetch(ptr,std::arch::x86_64::_MM_HINT_T0);
                std::arch::x86_64::_mm_prefetch(ptr.add(len-1),std::arch::x86_64::_MM_HINT_T0);
            }
        }
    }
    #[inline(always)]
    pub fn seconds(&self,stored:u64,seconds:u32)->i64 {
        if stored<RARE_TIME {seconds as i64} else {self.times.lock().unwrap()[(stored&((1<<41)-1)) as usize].1}
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
    fn concurrent_opaque_event_ids_are_exact() {
        let tokens=Tokens::default();
        let names:Vec<&'static [u8]>=(0..1024).map(|i|&*Box::leak(format!("e{i:032x}").into_bytes().into_boxed_slice())).collect();
        let keys:Vec<u64>=names.iter().map(|v|tokens.key(v)).collect();
        assert_eq!(keys.iter().copied().collect::<std::collections::HashSet<_>>().len(),names.len());
        std::thread::scope(|scope| {
            for shift in 0..8 {
                let tokens=&tokens;let names=&names;let keys=&keys;
                scope.spawn(move||for i in 0..names.len()*4 {
                    let at=(i+shift*113)%names.len();let sec=if i%2==0 {-1} else {1};
                    let (stored,compact,identity)=tokens.stamp(names[at],sec);
                    assert_eq!(identity,tokens.identity(keys[at]));assert_eq!(tokens.identity(stored),tokens.identity(keys[at]));
                    assert_eq!(tokens.seconds(stored,compact),sec);
                });
            }
        });
    }
    #[test]
    fn source_fields_and_owned_fallback_compare_full_bytes() {
        let blob:&'static [u8]=Box::leak(b"opaque-event-a|opaque-event-b|opaque-event-a".to_vec().into_boxed_slice());
        let tokens=Tokens::test_refs(blob,None);
        let (a,_,ha)=tokens.stamp(&blob[..14],1);
        let (b,_,hb)=tokens.stamp(&blob[15..29],1);
        let (again,_,ha2)=tokens.stamp(&blob[30..],1);
        assert_ne!(a,again);assert_eq!(ha,ha2);assert_ne!(ha,hb);
        assert!(tokens.same_id(a,again));assert!(!tokens.same_id(a,b));
        let owned=tokens.key(&blob[..14]);assert_ne!(owned,a);
        assert!(tokens.same_id(a,owned));assert_eq!(tokens.identity(a),tokens.identity(owned));
        for len in [0,1,63,64,65537] {
            let value:&'static [u8]=Box::leak(vec![b'!';len].into_boxed_slice());
            let tokens=Tokens::test_refs(value,None);
            let (stored,sec,hash)=tokens.stamp(value,-1);
            assert_eq!(tokens.seconds(stored,sec),-1);
            assert_eq!(hash,tokens.identity(stored));
            assert_eq!(tokens.bytes(tokens.raw_identity(stored)),value);
            assert_eq!(tokens.raw_identity(stored)&63!=0,(1..=63).contains(&len));
        }
    }
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
