//! Keep contiguous literal-person runs from direct dedup slots. Run descriptors
//! are grouped, rather than hashing and redistributing every surviving event.
//! No identity or time order is inferred from an identifier's numeric value.
use crate::{Ctx,Agg,Scratch,Ev2,Ck,bin_of,person_sessions,p2,person_bucket};

#[derive(Default)]
pub struct Batch {pub(crate) values:Vec<u64>,pub(crate) spans:Vec<Span>}
#[derive(Clone,Copy)]
pub(crate) struct Span {pub(crate) key:u64,pub(crate) begin:u32,pub(crate) end:u32}
impl Batch {
    #[inline]
    pub fn push(&mut self,key:u64,value:u64) {
        let at=self.values.len() as u32;
        if let Some(last)=self.spans.last_mut().filter(|s|s.key==key) {last.end=at+1;}
        else {self.spans.push(Span{key,begin:at,end:at+1});}
        self.values.push(value);
    }
}
#[derive(Clone,Copy)]
struct Reference {key:u64,batch:u32,begin:u32,end:u32}
pub struct Runs {batches:Vec<Batch>,buckets:Vec<Vec<Reference>>,counts:Vec<usize>}
impl Runs {
    pub fn new(batches:Vec<Batch>)->Self {
        let start=std::time::Instant::now();
        let mut buckets:Vec<Vec<Reference>>=(0..p2()).map(|_|Vec::new()).collect();
        let mut counts=vec![0;p2()];
        for (index,batch) in batches.iter().enumerate() {
            for span in &batch.spans {
                let bucket=person_bucket(span.key);counts[bucket]+=(span.end-span.begin) as usize;
                buckets[bucket].push(Reference{key:span.key,batch:index as u32,begin:span.begin,end:span.end});
            }
        }
        use rayon::prelude::*;
        buckets.par_iter_mut().for_each(|b|b.sort_unstable_by_key(|r|r.key));
        if std::env::var_os("SS_TIMING").is_some() {
            eprintln!("dense runs: {} records, {} descriptors, index {:.1} ms",counts.iter().sum::<usize>(),buckets.iter().map(Vec::len).sum::<usize>(),start.elapsed().as_secs_f64()*1000.0);
        }
        Self{batches,buckets,counts}
    }
    pub fn count(&self,bucket:usize)->usize {self.counts[bucket]}
}

pub fn process_bucket(ctx:&Ctx,agg:&mut Agg,sc:&mut Scratch,generic:&mut [Ev2],clicks:&mut [Ck],runs:&Runs,bucket:usize) {
    static GROUPED:std::sync::OnceLock<bool>=std::sync::OnceLock::new();
    let grouped=*GROUPED.get_or_init(||std::env::var("SS_RUN_CLICKS").map_or(true,|v|v!="0"));
    process_impl(ctx,agg,sc,generic,clicks,runs,bucket,grouped);
}

fn grouped_clicks(sc:&mut Scratch,clicks:&[Ck],refs:&[Reference],generic:&[Ev2])->Vec<Ck> {
    let mut keys:Vec<_>=refs.iter().map(|r|r.key).chain(generic.iter().map(|e|e.key)).collect();
    keys.sort_unstable();keys.dedup();
    let cap=(keys.len()*2).next_power_of_two().max(16);let mask=cap-1;
    sc.table.resize(sc.table.len().max(cap),(0,u32::MAX));sc.used.clear();
    let slot_of=|key:u64|(key.wrapping_mul(0xd6e8_feb8_6659_fd93)>>32) as usize&mask;
    for (id,&key) in keys.iter().enumerate() {
        let mut slot=slot_of(key);while sc.table[slot].1!=u32::MAX {slot=(slot+1)&mask;}
        sc.table[slot]=(key,id as u32);sc.used.push(slot as u32);
    }
    sc.ck_cnt.clear();sc.ck_cnt.resize(keys.len()+1,0);sc.ck_local.clear();
    for click in clicks {
        let mut slot=slot_of(click.key);
        let id=loop {let entry=sc.table[slot];if entry.1==u32::MAX {break u32::MAX;}if entry.0==click.key {break entry.1;}slot=(slot+1)&mask;};
        if id!=u32::MAX {sc.ck_cnt[id as usize+1]+=1;}sc.ck_local.push(id);
    }
    for &slot in &sc.used {sc.table[slot as usize]=(0,u32::MAX);}
    for id in 0..keys.len() {sc.ck_cnt[id+1]+=sc.ck_cnt[id];}
    let n=*sc.ck_cnt.last().unwrap() as usize;let mut positions=sc.ck_cnt[..keys.len()].to_vec();
    let mut out=std::mem::take(&mut sc.cks_sorted);out.clear();out.reserve(n);
    for (click,&id) in clicks.iter().zip(&sc.ck_local) {
        if id==u32::MAX {continue;}
        let at=&mut positions[id as usize];unsafe {out.as_mut_ptr().add(*at as usize).write(*click);}*at+=1;
    }
    unsafe {out.set_len(n);}
    for id in 0..keys.len() {
        out[sc.ck_cnt[id] as usize..sc.ck_cnt[id+1] as usize].sort_unstable_by(|a,b|a.ts.cmp(&b.ts).then_with(||crate::deref(a.cref).cmp(crate::deref(b.cref))));
    }
    out
}

fn process_impl(ctx:&Ctx,agg:&mut Agg,sc:&mut Scratch,generic:&mut [Ev2],clicks:&mut [Ck],runs:&Runs,bucket:usize,hash_clicks:bool) {
    let refs=&runs.buckets[bucket];
    if refs.is_empty()&&generic.is_empty() {return;}
    generic.sort_unstable_by_key(|e|e.key);
    let mut grouped=if hash_clicks {grouped_clicks(sc,clicks,refs,generic)} else {Vec::new()};
    let clicks=if hash_clicks {&mut grouped[..]} else {
        clicks.sort_unstable_by(|a,b|a.key.cmp(&b.key).then_with(||a.ts.cmp(&b.ts)).then_with(||crate::deref(a.cref).cmp(crate::deref(b.cref))));clicks
    };
    let mut events=std::mem::take(&mut sc.evs_sorted);let mut temporary=std::mem::take(&mut sc.tmp_ev);
    let (mut r,mut e,mut c)=(0,0,0);
    while r<refs.len()||e<generic.len() {
        let key=match (refs.get(r),generic.get(e)) {
            (Some(a),Some(b))=>a.key.min(b.key),(Some(a),None)=>a.key,(None,Some(b))=>b.key,_=>unreachable!()
        };
        events.clear();
        while r<refs.len()&&refs[r].key==key {
            let span=refs[r];let values=&runs.batches[span.batch as usize].values[span.begin as usize..span.end as usize];
            events.extend(values.iter().map(|&v|Ev2{key,ts:(v>>32) as u32,pc:v as u32}));r+=1;
        }
        let begin=e;while e<generic.len()&&generic[e].key==key {e+=1;}
        events.extend_from_slice(&generic[begin..e]);
        // Sessionization needs ascending half-hour bins; validate that order
        // and counting-sort whenever independent runs or dirty rows break it.
        if events.windows(2).any(|w|bin_of(w[0].ts)>bin_of(w[1].ts)) {
            sc.bin_cnt.clear();sc.bin_cnt.resize(ctx.nbins,0);
            for event in &events {sc.bin_cnt[bin_of(event.ts)]+=1;}
            let mut total=0;
            for count in &mut sc.bin_cnt {let n=*count;*count=total;total+=n;}
            temporary.clear();temporary.reserve(events.len());
            for event in &events {
                let at=&mut sc.bin_cnt[bin_of(event.ts)];
                // Prefix positions are disjoint and cover exactly events.len().
                unsafe {temporary.as_mut_ptr().add(*at as usize).write(*event);}
                *at+=1;
            }
            unsafe {temporary.set_len(events.len());}
            std::mem::swap(&mut temporary,&mut events);
        }
        while c<clicks.len()&&clicks[c].key<key {c+=1;}
        let begin=c;while c<clicks.len()&&clicks[c].key==key {c+=1;}
        person_sessions(ctx,&events,&clicks[begin..c],agg,sc);
    }
    sc.evs_sorted=events;sc.tmp_ev=temporary;
    if hash_clicks {sc.cks_sorted=grouped;}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn run_boundaries_preserve_every_value() {
        let mut batch=Batch::default();
        for (key,value) in [(2,10),(2,11),(9,12),(2,13),(2,14)] {batch.push(key,value);}
        assert_eq!(batch.spans.len(),3);
        let rebuilt:Vec<_>=batch.spans.iter().flat_map(|span|batch.values[span.begin as usize..span.end as usize].iter().map(move|&v|(span.key,v))).collect();
        assert_eq!(rebuilt,[(2,10),(2,11),(9,12),(2,13),(2,14)]);
    }
    #[test]
    fn fragmented_dense_and_generic_events_match_general_aggregation() {
        let _guard=crate::tests_support::LOCK.lock().unwrap_or_else(|e|e.into_inner());
        let persons=crate::Persons{map:crate::PersonMap::build(&[]),names:Vec::new()};
        let ctx=Ctx{ndays:3,nbins:145,ncamp:3,ncat:2,ws:0,ws_day:0,persons:&persons};
        let make=||Agg{rows:Vec::new(),credit:vec![(0,0);12],assist:vec![0;12]};
        let mut general=vec![Vec::new();p2()];let mut generic=vec![Vec::new();p2()];
        let mut batches=Vec::new();let mut seed=91u64;
        for batch_index in 0..24 {
            let mut batch=Batch::default();
            for i in 0..2400 {
                seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;
                let key=if i%9<4 {7} else {seed%23+1};
                let ts=(seed%250_000_000) as u32;
                let pc=if seed%7==0 {((seed%4000+1) as u32)<<8|(seed as u32&1)} else {0};
                let event=Ev2{key,ts,pc};general[person_bucket(key)].push(event);
                if (i+batch_index)%13==0 {generic[person_bucket(key)].push(event);}
                else {batch.push(key,((ts as u64)<<32)|pc as u64);}
            }
            batches.push(batch);
        }
        let runs=Runs::new(batches);let mut expected=make();let mut actual=make();let mut grouped=make();
        let mut first=Scratch::new(3);let mut second=Scratch::new(3);let mut third=Scratch::new(3);
        for b in 0..p2() {
            let mut clicks:Vec<_>=(1..=23).filter(|&key|person_bucket(key)==b).flat_map(|key|(0..4).map(move|t|Ck{key,cref:0,ts:100_000_000+t*20_000_000,camp:t%3})).collect();
            crate::process_bucket(&ctx,&mut expected,&mut first,&general[b],&clicks);
            process_impl(&ctx,&mut actual,&mut second,&mut generic[b],&mut clicks,&runs,b,false);
            process_impl(&ctx,&mut grouped,&mut third,&mut generic[b],&mut clicks,&runs,b,true);
        }
        let rows=|agg:Agg| {let mut rows:Vec<_>=agg.rows.into_iter().map(|r|(r.name,r.day,r.sessions,r.events,r.revenue,r.top)).collect();rows.sort();(rows,agg.credit,agg.assist)};
        let expected=rows(expected);assert_eq!(expected,rows(actual));assert_eq!(expected,rows(grouped));
    }
}
