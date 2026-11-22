//! Range-partitioned direct event slots. IDs select only storage positions;
//! all identity edges and session rules remain literal and general.
use std::sync::{Mutex,atomic::{AtomicBool,Ordering}};
use rayon::prelude::*;
use crate::{model::Model,dense_user::Names,flow,Ev2,profile};
const USER_BITS:u32=28;
const USER_MASK:u64=(1<<USER_BITS)-1;
const EMPTY:u64=u64::MAX;
#[derive(Clone,Copy,Default)]
#[repr(C)]
pub struct Record {pub a:u64,pub b:u64}
#[derive(Clone,Copy,Default)]
#[repr(C)]
pub struct InlineRecord {pub record:Record,pub payload:u64}
#[derive(Clone,Copy)]
struct Wide {seconds:i64,user:u32}
pub struct Context {
    pub model:Model,
    pub width:u32,
    base:i64,
    wide:Mutex<Vec<Wide>>,
    pub failed:AtomicBool,
    pub defer_users:bool,
    pub user_index_bits:u32,
    pub inline_users:bool,
}
impl Context {
    pub fn new(model:Model,window_start:i64)->Self {
        let width=if model.bound>(1<<25) {15} else {14};
        let threads=rayon::current_num_threads();let thread_bits=usize::BITS-(threads-1).leading_zeros();
        Self{model,width,base:window_start.div_euclid(1000)-2*86400,wide:Mutex::new(Vec::new()),failed:AtomicBool::new(false),
            defer_users:threads<=128&&std::env::var("SS_DEFER_USER").map_or(true,|v|v!="0"),user_index_bits:USER_BITS-thread_bits.min(USER_BITS),
            inline_users:std::env::var("SS_INLINE_USER").map_or(true,|v|v!="0")}
    }
    pub fn parts(&self)->usize {(self.model.bound as usize).div_ceil(1<<self.width)}
    pub fn record(&self,index:u32,seconds:i64,user:u32,ts:u32,pc:u32)->Record {
        let shift=USER_BITS+self.width;let escape=(1u64<<(64-shift))-1;
        let relative=seconds.checked_sub(self.base);
        let (time,who)=if let Some(relative)=relative.filter(|&s|s>=0&&(s as u64)<escape) {
            (relative as u64,user as u64)
        } else {
            crate::profile::lock_count(2);let mut wide=self.wide.lock().unwrap();let id=wide.len();
            if id>=USER_MASK as usize {self.failed.store(true,Ordering::Relaxed);return Record{a:(u32::MAX as u64)<<32,b:0};}
            wide.push(Wide{seconds,user});(escape,id as u64)
        };
        assert!(who<=USER_MASK);
        Record{a:((ts as u64)<<32)|pc as u64,b:(time<<shift)|(((index as u64)&((1<<self.width)-1))<<USER_BITS)|who}
    }
    pub fn freeze(self,names:Names)->Frozen {Frozen{width:self.width,base:self.base,wide:self.wide.into_inner().unwrap(),names,defer_users:self.defer_users,user_index_bits:self.user_index_bits,inline_users:self.inline_users}}
}
pub struct Frozen {width:u32,base:i64,wide:Vec<Wide>,names:Names,defer_users:bool,user_index_bits:u32,inline_users:bool}
struct Resolver<'a> {frozen:&'a Frozen,sources:&'a [flow::Sink],users:&'a crate::dense_user::UserMap,previous:u32,key:u64,cache:[[u64;2];128]}
impl<'a> Resolver<'a> {
    fn new(frozen:&'a Frozen,sources:&'a [flow::Sink],users:&'a crate::dense_user::UserMap)->Self {Self{frozen,sources,users,previous:u32::MAX,key:0,cache:[[0;2];128]}}
    #[inline]
    fn key(&mut self,record:Record,inline_payload:u64)->u64 {
        let code=self.frozen.user(record);
        if !self.frozen.defer_users {
            if code!=self.previous {self.previous=code;self.key=self.frozen.names.person_key(code);}
            return self.key;
        }
        let bits=self.frozen.user_index_bits;
        let payload=if self.frozen.inline_users {inline_payload} else {self.sources[(code>>bits) as usize].deferred_users[(code&((1<<bits)-1)) as usize]};
        let slot=(payload.wrapping_mul(0xd6e8_feb8_6659_fd93)>>57) as usize;
        if self.cache[slot][0]==payload {return self.cache[slot][1];}
        crate::profile::lock_count(11);let mut buffer=[0u8;9];let bytes=crate::ids::bytes(payload,&mut buffer);
        let key=self.users.person_index(self.users.model.user(bytes),bytes);
        self.cache[slot]=[payload,key];key
    }
}
impl Frozen {
    #[inline]
    fn seconds(&self,r:Record)->i64 {
        let shift=USER_BITS+self.width;let escape=(1u64<<(64-shift))-1;let code=r.b>>shift;
        if code==escape {self.wide[(r.b&USER_MASK) as usize].seconds} else {self.base+code as i64}
    }
    #[inline]
    fn user(&self,r:Record)->u32 {
        let shift=USER_BITS+self.width;let escape=(1u64<<(64-shift))-1;
        if r.b>>shift==escape {self.wide[(r.b&USER_MASK) as usize].user} else {(r.b&USER_MASK) as u32}
    }
    fn earlier(&self,a:Record,b:Record)->bool {
        let shift=USER_BITS+self.width;let escape=(1u64<<(64-shift))-1;
        let x=a.b>>shift;let y=b.b>>shift;
        if x<escape&&y<escape {x<y} else {self.seconds(a)<self.seconds(b)}
    }
    fn visit(&self,part:usize,sources:&[flow::Sink],mut each:impl FnMut(Record,u64)) {
        if self.inline_users {
            INLINE_SLOTS.with(|cell| {
                let mut slots=cell.borrow_mut();let width=1usize<<self.width;
                slots.resize(width,InlineRecord{record:Record{a:0,b:EMPTY},payload:0});
                for source in sources {
                    for chunk in source.inline_records(part) {
                        for &record in chunk {
                            let index=((record.record.b>>USER_BITS) as usize)&(width-1);let old=slots[index];
                            if old.record.b==EMPTY||self.earlier(record.record,old.record) {slots[index]=record;}
                        }
                    }
                }
                for slot in &mut *slots {
                    let value=*slot;if value.record.b==EMPTY {continue;}slot.record.b=EMPTY;
                    if (value.record.a>>32) as u32!=u32::MAX {each(value.record,value.payload);}
                }
            });return;
        }
        SLOTS.with(|cell| {
            let mut slots=cell.borrow_mut();let width=1usize<<self.width;
            slots.resize(width,Record{a:0,b:EMPTY});
            for source in sources {
                for chunk in source.dense_records(part) {
                    for &record in chunk {
                        let index=((record.b>>USER_BITS) as usize)&(width-1);
                        let old=slots[index];
                        if old.b==EMPTY||self.earlier(record,old) {slots[index]=record;}
                    }
                }
            }
            for slot in &mut *slots {
                let record=*slot;if record.b==EMPTY {continue;}slot.b=EMPTY;
                if (record.a>>32) as u32!=u32::MAX {each(record,0);}
            }
        });
    }
    pub fn runs(&self,parts:usize,sources:&[flow::Sink],users:&crate::dense_user::UserMap)->crate::dense_runs::Runs {
        let phase=crate::profile::Span::new("dense_run_collect",0);
        let start=std::time::Instant::now();
        let batches=if std::env::var("SS_RUN_ARENA").map_or(true,|v|v!="0") {
            let width=1usize<<self.width;
            let mut values=vec![0;parts*width];
            let spans:Vec<Vec<crate::dense_runs::Span>>=values.par_chunks_mut(width).enumerate().map(|(part,dst)| {
                let mut spans:Vec<crate::dense_runs::Span>=Vec::new();let mut resolver=Resolver::new(self,sources,users);let mut n=0;
                self.visit(part,sources,|record,payload| {
                    let key=resolver.key(record,payload);
                    let index=(part*width+n) as u32;dst[n]=record.a;n+=1;
                    if let Some(last)=spans.last_mut().filter(|s|s.key==key) {last.end=index+1;}
                    else {spans.push(crate::dense_runs::Span{key,begin:index,end:index+1});}
                });spans
            }).collect();
            vec![crate::dense_runs::Batch{values,spans:spans.into_iter().flatten().collect()}]
        } else {(0..parts).into_par_iter().map(|part| {
            let mut batch=crate::dense_runs::Batch::default();
            let mut resolver=Resolver::new(self,sources,users);
            self.visit(part,sources,|record,payload| {
                let key=resolver.key(record,payload);
                batch.push(key,record.a);
            });batch
        }).collect()};
        phase.finish(0,0);
        if std::env::var_os("SS_TIMING").is_some() {eprintln!("dense run collect: {:.1} ms",start.elapsed().as_secs_f64()*1000.0);}
        crate::dense_runs::Runs::new(batches)
    }
    pub fn emit(&self,parts:usize,sources:&[flow::Sink],workers:usize,users:&crate::dense_user::UserMap)->Vec<flow::Sink> {
        let outputs:Vec<Mutex<flow::Sink>>=(0..workers).map(|_|Mutex::new(flow::Sink::new())).collect();
        (0..parts).into_par_iter().for_each(|part| {
            let span=profile::Span::new("dense_event_bucket",part);
            let worker=rayon::current_thread_index().unwrap_or(0)%workers;
            let mut output=outputs[worker].lock().unwrap();let mut resolver=Resolver::new(self,sources,users);let mut n=0;
            self.visit(part,sources,|record,payload| {
                let key=resolver.key(record,payload);
                output.emit_dense(Ev2{key,ts:(record.a>>32) as u32,pc:record.a as u32});n+=1;
            });span.finish(n,n*16);
        });
        let mut outputs:Vec<_>=outputs.into_iter().map(|m|m.into_inner().unwrap()).collect();
        outputs.par_iter_mut().for_each(flow::Sink::finish);outputs
    }
}
thread_local! {static SLOTS:std::cell::RefCell<Vec<Record>>=const{std::cell::RefCell::new(Vec::new())};}
thread_local! {static INLINE_SLOTS:std::cell::RefCell<Vec<InlineRecord>>=const{std::cell::RefCell::new(Vec::new())};}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_ingestion_and_slot_fields_are_lossless() {
        let model=Model{prefix:b'e',key:1,bound:100000};let ctx=Context::new(model,1776211200000);
        let times=[-62167219200,-1,0,1776211200,1776211200+30*86400,253402300799];
        let mut records=Vec::new();
        for (i,&seconds) in times.iter().enumerate() {records.push(ctx.record(i as u32,seconds,71,912,987));}
        let names=crate::dense_user::Names::for_test(model);
        let frozen=ctx.freeze(names);
        for (i,&r) in records.iter().enumerate() {assert_eq!(frozen.seconds(r),times[i]);assert_eq!(frozen.user(r),71);assert_eq!((r.a>>32) as u32,912);assert_eq!(r.a as u32,987);}
        for i in 1..records.len() {assert!(frozen.earlier(records[i-1],records[i]));}
        assert_eq!(std::mem::size_of::<Record>(),16);assert_eq!(std::mem::size_of::<InlineRecord>(),24);
    }
    #[test]
    fn deferred_payloads_preserve_links_fallbacks_and_cache_collisions() {
        let _guard=crate::tests_support::LOCK.lock().unwrap_or_else(|e|e.into_inner());
        let user_model=Model{prefix:b'u',key:0x82732f912,bound:32};let mut labels=Vec::new();
        for i in 0..160 {let mut buf=[0;10];let n=user_model.label(((i/8)<<9)|(i&7),&mut buf);labels.push(buf[..n].to_vec());}
        labels.push("strange-\u{e9}".as_bytes().to_vec());labels.push("\u{e9}".repeat(40000).into_bytes());labels.push(b"u000001".to_vec());
        let mut data=b"user_id_a,user_id_b\n".to_vec();
        for (a,b) in [(0,1),(1,160),(160,161),(2,24),(24,162)] {data.extend_from_slice(&labels[a]);data.push(b',');data.extend_from_slice(&labels[b]);data.push(b'\n');}
        crate::tests_support::install(&data);
        let input=crate::Inputs{events:vec![],clicks:vec![],links:vec![0],products:0,campaigns:0,window_start:0,window_end:1};
        let people=crate::dense_user::build(&input,user_model).unwrap();let users=people.map.dense.as_ref().unwrap();
        let event_model=Model{prefix:b'e',key:7,bound:100000};let mut ctx=Context::new(event_model,1776211200000);ctx.defer_users=true;ctx.inline_users=false;ctx.user_index_bits=27;
        let mut sources=vec![flow::Sink::new(),flow::Sink::new()];let mut records=Vec::new();
        for (i,label) in labels.iter().enumerate() {
            let thread=i%2;let offset=sources[thread].deferred_users.len();sources[thread].deferred_users.push(crate::ids::payload_of(label));
            records.push(ctx.record(i as u32,if i%7==0 {-1} else {1776211200},((thread as u32)<<27)|offset as u32,7,99));
        }
        let mut frozen=ctx.freeze(users.snapshot());
        for inline in [false,true] {frozen.inline_users=inline;let mut resolver=Resolver::new(&frozen,&sources,users);
        for _ in 0..8 {for (label,&record) in labels.iter().zip(&records) {
            let hash=crate::h64(label);let expected=people.map.get(hash).unwrap_or_else(||crate::unlinked_key(hash));
            assert_eq!(resolver.key(record,crate::ids::payload_of(label)),expected);
        }}}
    }
}
