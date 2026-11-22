//! Exact identity graph indexed by reversible common labels and a sparse
//! dictionary. The decoded group/alias values never imply an identity edge.
use std::collections::HashMap;
use std::sync::{Mutex,atomic::{AtomicBool,AtomicU32,AtomicU64,Ordering}};
use rayon::prelude::*;
use crate::{model::Model,Inputs,Persons,PersonMap,LINKED,file_bytes,for_records,mkref,deref,h64,ids,chunks_of};
pub const LINK_CODE:u32=1<<27;
#[derive(Default)]
struct Registry {by_key:HashMap<u64,u32>,names:Vec<u64>}
pub struct UserMap {
    pub model:Model,
    common:usize,
    component:Vec<AtomicU32>, // zero: unlinked; otherwise component index + 1
    registry:Mutex<Registry>,
    pub linked_count:usize,
}
impl UserMap {
    fn vertex(&self,bytes:&[u8],create:bool)->Option<u32> {
        if let Some(index)=self.model.user(bytes) {return Some(index);}
        let payload=ids::unmix(h64(bytes));let mut registry=self.registry.lock().unwrap();
        if let Some(&id)=registry.by_key.get(&payload) {return Some(id);}
        if !create {return None;}
        let id=self.common.checked_add(registry.names.len())?;
        if id>=LINK_CODE as usize {return None;}
        registry.names.push(payload);registry.by_key.insert(payload,id as u32);Some(id as u32)
    }
    pub fn prefetch_index(&self,index:Option<u32>) {
        #[cfg(target_arch="x86_64")]
        if let Some(index)=index.filter(|&i|(i as usize)<self.component.len()) {
            unsafe {std::arch::x86_64::_mm_prefetch(self.component.as_ptr().add(index as usize).cast(),std::arch::x86_64::_MM_HINT_T0);}
        }
    }
    pub fn person_index(&self,index:Option<u32>,bytes:&[u8])->u64 {
        let index=index.or_else(||self.vertex(bytes,false));
        let comp=index.and_then(|i|self.component.get(i as usize)).map_or(0,|x|x.load(Ordering::Relaxed));
        if comp==0 {ids::payload_of(bytes)} else {LINKED|(comp-1) as u64}
    }
    #[inline]
    pub fn code(&self,bytes:&[u8])->Option<u32> {self.code_index(self.model.user(bytes),bytes)}
    #[inline]
    pub fn code_index(&self,index:Option<u32>,bytes:&[u8])->Option<u32> {
        let index=index.or_else(||self.vertex(bytes,true))? as usize;
        let comp=self.component.get(index).map_or(0,|x|x.load(Ordering::Relaxed));
        if comp==0 {Some(index as u32)} else {(comp-1<LINK_CODE).then_some(LINK_CODE|comp-1)}
    }
    pub fn get_hash(&self,hash:u64)->Option<u64> {
        let mut buffer=[0u8;9];let bytes=ids::bytes(ids::unmix(hash),&mut buffer);
        let index=self.vertex(bytes,false)? as usize;
        let comp=self.component.get(index)?.load(Ordering::Relaxed);
        (comp!=0).then_some(LINKED|comp.wrapping_sub(1) as u64)
    }
    pub fn prefetch_hash(&self,hash:u64) {
        let mut buffer=[0u8;9];let bytes=ids::bytes(ids::unmix(hash),&mut buffer);
        if let Some(index)=self.model.user(bytes) {
            #[cfg(target_arch="x86_64")]
            unsafe {std::arch::x86_64::_mm_prefetch(self.component.as_ptr().add(index as usize).cast(),std::arch::x86_64::_MM_HINT_T0);}
        }
    }
    pub fn snapshot(&self)->Names {Names{model:self.model,common:self.common,extra:self.registry.lock().unwrap().names.clone()}}
}
pub struct Names {model:Model,common:usize,extra:Vec<u64>}
impl Names {
    #[cfg(test)]
    pub fn for_test(model:Model)->Self {Self{model,common:model.bound as usize*8,extra:Vec::new()}}
    pub fn person_key(&self,code:u32)->u64 {
        if code&LINK_CODE!=0 {return LINKED|(code&!LINK_CODE) as u64;}
        let index=code as usize;
        if index>=self.common {return self.extra[index-self.common];}
        let number=((index as u64>>3)<<9)|(index as u64&7);
        let mut label=[0u8;10];let n=self.model.label(number,&mut label);
        ids::payload_of(&label[..n])
    }
}
#[inline]
fn root(parent:&[AtomicU32],mut x:u32)->u32 {
    loop {
        let p=parent[x as usize].load(Ordering::Relaxed);
        if p==0 {return x;}
        let y=p-1;let gp=parent[y as usize].load(Ordering::Relaxed);
        if gp!=0 {let _=parent[x as usize].compare_exchange_weak(p,gp,Ordering::Relaxed,Ordering::Relaxed);}
        x=y;
    }
}
#[derive(Clone,Copy)]
struct Endpoint {vertex:u32,name:u64}

pub fn build(input:&Inputs,model:Model)->Option<Persons> {
    let common=(model.bound as usize).checked_mul(8)?;
    if common>=LINK_CODE as usize {return None;}
    let mut map=UserMap{model,common,component:Vec::new(),registry:Mutex::new(Registry::default()),linked_count:0};
    let failed=AtomicBool::new(false);
    let parts:Vec<Vec<Endpoint>>=chunks_of(&input.links,256<<10).par_iter().map(|&(file,s,e)| {
        let mut endpoints=Vec::new();
        for_records::<2>(file,s,e,|p,line,fs| {
            let Some(fs)=fs else {return;};let (a,b)=fs.get(0);let (c,d)=fs.get(1);
            if a==b||c==d {return;}
            let (Some(va),Some(vb))=(map.vertex(&line[a..b],true),map.vertex(&line[c..d],true)) else {failed.store(true,Ordering::Relaxed);return;};
            endpoints.push(Endpoint{vertex:va,name:mkref(file,p+a,b-a)});
            endpoints.push(Endpoint{vertex:vb,name:mkref(file,p+c,d-c)});
        });endpoints
    }).collect();
    if failed.load(Ordering::Relaxed) {return None;}
    let endpoints:Vec<Endpoint>=parts.into_iter().flatten().collect();
    if endpoints.len()>=u32::MAX as usize {return None;}
    let count=common+map.registry.lock().unwrap().names.len();
    let parent:Vec<AtomicU32>=(0..count).into_par_iter().map(|_|AtomicU32::new(0)).collect();
    let active:Vec<AtomicU64>=(0..count.div_ceil(64)).map(|_|AtomicU64::new(0)).collect();
    endpoints.par_chunks_exact(2).for_each(|edge| {
        let (mut a,mut b)=(edge[0].vertex,edge[1].vertex);
        for x in [a,b] {active[x as usize/64].fetch_or(1u64<<(x%64),Ordering::Relaxed);}
        loop {
            a=root(&parent,a);b=root(&parent,b);if a==b {break;}
            let (hi,lo)=if a>b {(a,b)} else {(b,a)};
            if parent[hi as usize].compare_exchange(0,lo+1,Ordering::AcqRel,Ordering::Relaxed).is_ok() {break;}
        }
    });
    let vertices:Vec<u32>=active.par_iter().enumerate().flat_map_iter(|(word,bits)| {
        let mut b=bits.load(Ordering::Relaxed);
        std::iter::from_fn(move || {if b==0 {None} else {let bit=b.trailing_zeros();b&=b-1;Some((word*64+bit as usize) as u32)}})
    }).collect();
    let roots:Vec<u32>=vertices.par_iter().map(|&v|root(&parent,v)).collect();
    let best:Vec<AtomicU32>=(0..count).into_par_iter().map(|_|AtomicU32::new(0)).collect();
    endpoints.par_iter().enumerate().for_each(|(i,e)| {
        let r=root(&parent,e.vertex) as usize;let mut old=best[r].load(Ordering::Relaxed);
        loop {
            if old!=0&&deref(e.name)>=deref(endpoints[old as usize-1].name) {break;}
            match best[r].compare_exchange_weak(old,i as u32+1,Ordering::Relaxed,Ordering::Relaxed) {Ok(_)=>break,Err(value)=>old=value}
        }
    });
    let mut names=Vec::new();let mut comp=vec![0u32;count];
    for (&v,&r) in vertices.iter().zip(&roots) {
        if v==r {
            if names.len()>=LINK_CODE as usize {return None;}
            let at=best[r as usize].load(Ordering::Relaxed);assert_ne!(at,0);
            names.push(endpoints[at as usize-1].name);comp[r as usize]=names.len() as u32;
        }
    }
    vertices.par_iter().zip(&roots).for_each(|(&v,&r)|parent[v as usize].store(comp[r as usize],Ordering::Relaxed));
    map.linked_count=vertices.len();map.component=parent;
    Some(Persons{map:PersonMap{shift:0,sub:0,slots:Vec::new(),dense:Some(Box::new(map))},names})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn literal_graph_names_and_unlinked_codes_are_exact() {
        let _guard=crate::tests_support::LOCK.lock().unwrap_or_else(|e|e.into_inner());
        let model=Model{prefix:b'u',key:0x98127346,bound:32};
        let mut ids=Vec::new();
        for index in [0,1,2,7,512,513,1024,1025,1536,2048,4096] {
            let mut label=[0;10];let n=model.label(index,&mut label);ids.push(label[..n].to_vec());
        }
        ids.push("unicode-\u{e9}".as_bytes().to_vec());
        ids.push("\u{e9}".repeat(40000).into_bytes());
        ids.push(b"u0000000".to_vec());
        let edges=[(0,1),(1,2),(2,4),(4,5),(5,11),(11,12),(6,7),(8,8),(9,13)];
        let mut data=b"user_id_a,user_id_b\n".to_vec();
        for &(a,b) in &edges {data.extend_from_slice(&ids[a]);data.push(b',');data.extend_from_slice(&ids[b]);data.push(b'\n');}
        crate::tests_support::install(&data);
        let input=Inputs{events:vec![],clicks:vec![],links:vec![0],products:0,campaigns:0,window_start:0,window_end:1};
        let people=build(&input,model).unwrap();
        for start in 0..ids.len() {
            let mut seen=vec![false;ids.len()];let mut todo=vec![start];seen[start]=true;
            while let Some(v)=todo.pop() {
                for &(a,b) in &edges {
                    let other=if a==v {Some(b)} else if b==v {Some(a)} else {None};
                    if let Some(next)=other {if !seen[next] {seen[next]=true;todo.push(next);}}
                }
            }
            let linked=edges.iter().any(|&(a,b)|a==start||b==start);
            let actual=people.map.get(h64(&ids[start]));
            if linked {
                let key=actual.unwrap();assert_ne!(key&LINKED,0);
                let expected=ids.iter().enumerate().filter(|(i,_)|seen[*i]).map(|(_,v)|v).min().unwrap();
                assert_eq!(deref(people.names[(key&!LINKED) as usize]),expected);
            } else {assert_eq!(actual,None);}
        }
        let map=people.map.dense.as_ref().unwrap();
        let codes:Vec<_>=ids.iter().map(|id|map.code(id).unwrap()).collect();
        let names=map.snapshot();
        for (id,code) in ids.iter().zip(codes) {
            let key=names.person_key(code);
            if let Some(expected)=people.map.get(h64(id)) {assert_eq!(key,expected);} else {assert_eq!(key,crate::unlinked_key(h64(id)));}
        }
    }
}
