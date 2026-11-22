//! Data flow: links are resolved first, so every valid event and click is
//! written once, straight into its person partition. Dedup works on small copy
//! records partitioned by event id and only marks the losing copies dead.

use super::*;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;

pub const PD_BITS: u32 = 10; // event-id partitions of the copy records (default)
pub static PDB: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(PD_BITS);
/// Write-combining staging with non-temporal stores for the record streams.
pub const WC_DEFAULT: bool = true;
pub static WC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(WC_DEFAULT);

#[inline(always)]
pub fn pd_bits() -> u32 {
    PDB.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn pd() -> usize {
    1 << pd_bits()
}

/// One copy, including exact identity, compact ingestion and location:16 bytes.
#[derive(Clone,Copy,Default)]
#[repr(C)]
pub struct CopyR { a:u64,b:u64 }
impl CopyR {
    #[inline(always)]
    fn new(eid:u64,ingest:u32,loc:u64)->Self {
        debug_assert!(eid<(1<<48)&&loc<(1<<48));
        Self {a:eid|((ingest as u64&65535)<<48),b:(loc<<16)|((ingest as u64)>>16)}
    }
    #[inline(always)] fn eid(&self)->u64 {self.a&((1<<48)-1)}
    #[inline(always)] fn ingest(&self)->u32 {(self.a>>48) as u32|((self.b as u32&65535)<<16)}
    #[inline(always)] fn loc(&self)->u64 {self.b>>16}
}
const NOLOC:u64=0;
#[inline(always)]
fn eid_hash(eid:u64)->u64 {eid.wrapping_mul(0xD6E8_FEB8_6659_FD93).rotate_left(32)}
#[inline(always)]
fn eid_part(eid:u64)->usize {(eid_hash(eid)>>(64-pd_bits())) as usize}

// ---------------------------------------------------------------- block streams

const BLOCK_BYTES: usize = 8192;
const ARENA_CHUNK: usize = 8 << 20;

/// Bump allocator of fixed-size blocks out of huge-page chunks (never freed:
/// the process exits right after the run).
pub struct Arena {
    ptr: usize,
    end: usize,
    chunks: Vec<Vec<u8>>,
}

impl Arena {
    fn new() -> Self {
        Arena { ptr: 0, end: 0, chunks: Vec::new() }
    }
    #[cold]
    #[inline(never)]
    fn refill(&mut self) {
        let mut v: Vec<u8> = Vec::with_capacity(ARENA_CHUNK);
        self.ptr = v.as_mut_ptr() as usize;
        self.end = self.ptr + v.capacity();
        self.chunks.push(v);
    }
    #[inline(always)]
    fn block(&mut self) -> usize {
        if self.end - self.ptr < BLOCK_BYTES {
            self.refill();
        }
        let p = self.ptr;
        self.ptr += BLOCK_BYTES;
        p
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Tail {
    cur: usize,
    left: u32,
    n: u32,
}

/// Per-partition append-only lists of records in fixed-size blocks. Every
/// block is full except the last of a partition. With write combining, records
/// first gather in a small per-partition staging area and leave it a few cache
/// lines at a time with non-temporal stores (no read-for-ownership of the
/// destination, no cache pollution).
pub struct Stream<T> {
    tails: Vec<Tail>,
    blocks: Vec<Vec<usize>>,
    stage: Vec<T>,
    wc: bool,
    _t: PhantomData<T>,
}

impl<T: Copy + Default> Stream<T> {
    /// Records per staging flush: a whole number of cache lines.
    const K: usize = if std::mem::size_of::<T>() == 24 { 8 } else if std::mem::size_of::<T>() == 32 || std::mem::size_of::<T>() == 16 { 4 } else { 1 };
    /// Records per block, a multiple of K.
    const CAP: usize = BLOCK_BYTES / std::mem::size_of::<T>() / Self::K * Self::K;

    fn new(parts: usize) -> Self {
        let wc = WC.load(Ordering::Relaxed) && Self::K > 1;
        let stage = if wc { vec![T::default(); parts * Self::K] } else { Vec::new() };
        Stream { tails: vec![Tail::default(); parts], blocks: (0..parts).map(|_| Vec::new()).collect(), stage, wc, _t: PhantomData }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, arena: &mut Arena, p: usize) {
        let b = arena.block();
        self.blocks[p].push(b);
        let t = &mut self.tails[p];
        t.cur = b;
        t.left = Self::CAP as u32;
    }

    /// Append `v` to partition `p`; returns its index there.
    #[inline(always)]
    fn push(&mut self, arena: &mut Arena, p: usize, v: T) -> u32 {
        if self.wc {
            return self.push_wc(arena, p, v);
        }
        if unsafe { self.tails.get_unchecked(p) }.left == 0 {
            self.grow(arena, p);
        }
        let t = unsafe { self.tails.get_unchecked_mut(p) };
        unsafe { (t.cur as *mut T).write(v) };
        t.cur += std::mem::size_of::<T>();
        t.left -= 1;
        let i = t.n;
        t.n += 1;
        i
    }

    #[inline(always)]
    fn push_wc(&mut self, arena: &mut Arena, p: usize, v: T) -> u32 {
        let i = unsafe { self.tails.get_unchecked(p) }.n;
        let f = i as usize % Self::K;
        unsafe { *self.stage.get_unchecked_mut(p * Self::K + f) = v };
        unsafe { self.tails.get_unchecked_mut(p) }.n = i + 1;
        if f == Self::K - 1 {
            self.flush(arena, p);
        }
        i
    }

    /// Move partition p's full staging area to its block with streaming stores.
    #[inline(always)]
    fn flush(&mut self, arena: &mut Arena, p: usize) {
        if unsafe { self.tails.get_unchecked(p) }.left == 0 {
            self.grow(arena, p);
        }
        let t = unsafe { self.tails.get_unchecked_mut(p) };
        let bytes = Self::K * std::mem::size_of::<T>();
        unsafe {
            let src = self.stage.as_ptr().add(p * Self::K) as *const u8;
            let dst = t.cur as *mut u8;
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                use std::arch::x86_64::*;
                let mut o = 0;
                while o < bytes {
                    let x = _mm256_loadu_si256(src.add(o) as *const __m256i);
                    _mm256_stream_si256(dst.add(o) as *mut __m256i, x);
                    o += 32;
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
            std::ptr::copy_nonoverlapping(src, dst, bytes);
        }
        t.cur += bytes;
        t.left -= Self::K as u32;
    }

    /// Write out what is still staged (after the last push of the phase).
    fn finish(&mut self, arena: &mut Arena) {
        if !self.wc {
            return;
        }
        for p in 0..self.tails.len() {
            let rest = self.tails[p].n as usize % Self::K;
            if rest == 0 {
                continue;
            }
            if self.tails[p].left == 0 {
                self.grow(arena, p);
            }
            let t = &mut self.tails[p];
            unsafe {
                std::ptr::copy_nonoverlapping(self.stage.as_ptr().add(p * Self::K), t.cur as *mut T, rest);
            }
            t.cur += rest * std::mem::size_of::<T>();
            t.left -= rest as u32;
        }
    }

    #[inline]
    pub fn len(&self, p: usize) -> usize {
        self.tails[p].n as usize
    }

    pub fn total(&self)->usize {self.tails.iter().map(|t|t.n as usize).sum()}

    /// The records of partition `p`, block by block, in push order.
    #[inline]
    pub fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.tails[p].n as usize;
        self.blocks[p].iter().enumerate().map(move |(k, &b)| {
            let len = (n - k * Self::CAP).min(Self::CAP);
            unsafe { std::slice::from_raw_parts(b as *const T, len) }
        })
    }
}

/// One worker's output of the parse phase.
pub struct Sink {
    arena: Arena,
    pub ev: Stream<Ev2>,
    pub cp: Stream<CopyR>,
    pub ck: Stream<Ck>,
    dense: Option<Stream<crate::dense_events::Record>>,
    pub deferred_users: Vec<u64>,
    inline: Option<Stream<crate::dense_events::InlineRecord>>,
}

// raw block pointers into this sink's own arena
unsafe impl Send for Sink {}
unsafe impl Sync for Sink {}

impl Sink {
    pub fn new() -> Self {
        Sink { arena: Arena::new(), ev: Stream::new(p2()), cp: Stream::new(pd()), ck: Stream::new(p2()),dense:None,deferred_users:Vec::new(),inline:None }
    }
    pub fn enable_dense(&mut self,parts:usize,inline:bool) {if inline {self.inline=Some(Stream::new(parts));} else {self.dense=Some(Stream::new(parts));}}
    pub fn append_inline(&mut self,part:usize,record:crate::dense_events::InlineRecord) {self.inline.as_mut().unwrap().push(&mut self.arena,part,record);}
    pub fn inline_records(&self,part:usize)->impl Iterator<Item=&[crate::dense_events::InlineRecord]>+'_ {self.inline.iter().flat_map(move|s|s.slices(part))}
    pub fn append_dense(&mut self,part:usize,record:crate::dense_events::Record) {self.dense.as_mut().unwrap().push(&mut self.arena,part,record);}
    pub fn dense_total(&self)->usize {self.dense.as_ref().map_or(0,|d|d.total())+self.inline.as_ref().map_or(0,|d|d.total())}
    pub fn dense_records(&self,part:usize)->impl Iterator<Item=&[crate::dense_events::Record]>+'_ {self.dense.iter().flat_map(move |s|s.slices(part))}
    pub fn emit_dense(&mut self,event:Ev2) {self.ev.push(&mut self.arena,person_bucket(event.key),event);}
    /// End of the parse phase: everything staged goes to its blocks.
    pub fn finish(&mut self) {
        self.ev.finish(&mut self.arena);
        self.cp.finish(&mut self.arena);
        self.ck.finish(&mut self.arena);
        if let Some(dense)=&mut self.dense {dense.finish(&mut self.arena);}
        if let Some(inline)=&mut self.inline {inline.finish(&mut self.arena);}
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
    }
}

// ---------------------------------------------------------------- parse

#[derive(Clone, Copy, Default)]
struct PendE {
    uh: u64,
    eid: u64,
    part: u32,
    ingest: u32,
    ts: u32,
    pc: u32,
}

#[inline(always)]
fn emit_event(sink: &mut Sink, persons: &Persons, tokens: &crate::event_token::Tokens, tid: u64, pd: &PendE) {
    let key = persons.map.get(pd.uh).unwrap_or_else(|| unlinked_key(pd.uh));
    let pb = person_bucket(key);
    let i = sink.ev.push(&mut sink.arena, pb, Ev2 { key, ts: pd.ts, pc: pd.pc });
    let loc = tokens.location(tid as usize,pb,i as usize);
    sink.cp.push(&mut sink.arena, pd.part as usize, CopyR::new(pd.eid,pd.ingest,loc));
}

/// Parse one unit of event lines into `sink`.
#[allow(clippy::too_many_arguments)]
pub fn events_unit(f: usize, s: usize, e: usize, lk: &Lookups, persons: &Persons, tokens: &crate::event_token::Tokens, ws: i64, we: i64, sink: &mut Sink, tid: u64) {
    const AHEAD: usize = 16;
    let mut ring = [PendE::default(); AHEAD];
    let mut memo = parse::Memo::new();
    let mut head = 0usize;
    let mut filled = 0usize;
    crate::csv_local::records(f, s, e, |p, line, fs| {
        let Some(fs) = fs else { return };
        let fld = |i: usize| fs.get(line, i);
        let eid = fld(0);
        if eid.is_empty() {
            return;
        }
        let ingest_ms = parse::ingest_memo(fld(3), &mut memo).unwrap_or(i64::MAX / 2);
        let (eid_h,ingest,actual_eid) = tokens.stamp(eid,ingest_ms.div_euclid(1000));
        let part = eid_part(actual_eid) as u32;
        'valid: {
            let user = fld(1);
            if user.is_empty() {
                break 'valid;
            }
            let Some(ts) = parse::ts_memo(fld(2), &mut memo) else { break 'valid };
            if ts < ws || ts >= we || ingest_ms - ts > DAY_MS {
                break 'valid;
            }
            if fld(5).is_empty() {
                break 'valid;
            }
            let ty = fld(4);
            let pc = if ty == b"view" || ty == b"cart" {
                0
            } else if ty == b"purchase" {
                let Some(pr) = parse::price(fld(6)) else { break 'valid };
                let cat = lk.products.get(h64(fld(5))).map_or(lk.unknown_cat as u64, |c| c) as u32;
                (pr << 8) | cat
            } else {
                break 'valid;
            };
            let uh = h64(user);
            persons.map.prefetch(uh);
            let pd = PendE {
                uh,
                eid: eid_h,
                part,
                ingest,
                ts: (ts - ws) as u32,
                pc,
            };
            if filled == AHEAD {
                emit_event(sink, persons, tokens, tid, &ring[head]);
            } else {
                filled += 1;
            }
            ring[head] = pd;
            head = (head + 1) % AHEAD;
            return;
        }
        // an invalid copy still takes part in dedup
        sink.cp.push(&mut sink.arena, part as usize, CopyR::new(eid_h,ingest,NOLOC));
    });
    let start = if filled == AHEAD { head } else { 0 };
    for k in 0..filled {
        let pd = ring[(start + k) % AHEAD];
        emit_event(sink, persons, tokens, tid, &pd);
    }
}

#[derive(Clone, Copy, Default)]
struct PendC {
    uh: u64,
    cref: u64,
    ts: u32,
    camp: u32,
}

#[inline(always)]
fn emit_click(sink: &mut Sink, persons: &Persons, pd: &PendC) {
    let key = persons.map.get(pd.uh).unwrap_or_else(|| unlinked_key(pd.uh));
    sink.ck.push(&mut sink.arena, person_bucket(key), Ck { key, cref: pd.cref, ts: pd.ts, camp: pd.camp });
}

/// Parse one unit of click lines into `sink`.
#[allow(clippy::too_many_arguments)]
pub fn clicks_unit(f: usize, s: usize, e: usize, lk: &Lookups, persons: &Persons, ws: i64, we: i64, sink: &mut Sink) {
    const AHEAD: usize = 16;
    let lo = ws - CLICK_REACH;
    let mut ring = [PendC::default(); AHEAD];
    let mut memo = parse::Memo::new();
    let mut head = 0usize;
    let mut filled = 0usize;
    crate::scan::scan_records::<4>(f, s, e, |p, line, fs| {
        let Some(fs) = fs else { return };
        let fld = |i: usize| fs.get(line, i);
        let cid = fld(0);
        let user = fld(1);
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(ts) = parse::ts_memo(fld(3), &mut memo) else { return };
        if ts < lo || ts >= we {
            return;
        }
        let Some(camp) = lk.campaigns.get(h64(fld(2))) else { return };
        let uh = h64(user);
        persons.map.prefetch(uh);
        let pd = PendC { uh, cref: mkref(f, p + fs.span(line, 0).0, cid.len()), ts: (ts - lo) as u32, camp: camp as u32 };
        if filled == AHEAD {
            emit_click(sink, persons, &ring[head]);
        } else {
            filled += 1;
        }
        ring[head] = pd;
        head = (head + 1) % AHEAD;
    });
    let start = if filled == AHEAD { head } else { 0 };
    for k in 0..filled {
        let pd = ring[(start + k) % AHEAD];
        emit_click(sink, persons, &pd);
    }
}

// ---------------------------------------------------------------- dedup

/// Dead flags of event records: one bit per record of every (thread, person
/// partition) stream.
pub struct Dead {
    words: Vec<AtomicU64>,
    off: Vec<usize>, // [thread * np + partition] -> first word
    np: usize,
}

impl Dead {
    pub fn new(sinks: &[Sink]) -> Self {
        let np = p2();
        let mut off = Vec::with_capacity(sinks.len() * np);
        let mut n = 0usize;
        for s in sinks {
            for p in 0..np {
                off.push(n);
                n += s.ev.len(p).div_ceil(64);
            }
        }
        let words = (0..n).map(|_| AtomicU64::new(0)).collect();
        Dead { words, off, np }
    }
    #[inline]
    fn kill(&self, loc: u64,tokens: &crate::event_token::Tokens) {
        let (t,p,i) = tokens.decode_location(loc);
        self.words[self.off[t * self.np + p] + i / 64].fetch_or(1 << (i % 64), Ordering::Relaxed);
    }
    #[inline(always)]
    fn word(&self, t: usize, p: usize, w: usize) -> u64 {
        unsafe { self.words.get_unchecked(*self.off.get_unchecked(t * self.np + p) + w) }.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct DedupScratch2 {
    buf: Vec<CopyR>,
    table: Vec<u64>,
}

thread_local! {
    static DEDUP2: std::cell::RefCell<DedupScratch2> = std::cell::RefCell::new(DedupScratch2::default());
}

/// Keep the earliest-ingested copy of every event id in partition `b`; mark
/// the event records of all other copies dead.
pub fn dedup_part(b: usize, sinks: &[Sink], dead: &Dead, tokens: &crate::event_token::Tokens) {
    let span=profile::Span::new("dedup_bucket",b);
    DEDUP2.with(|cell| {
        let mut guard = cell.borrow_mut();
        let sc = &mut *guard;
        sc.buf.clear();
        for s in sinks {
            for sl in s.cp.slices(b) {
                sc.buf.extend_from_slice(sl);
            }
        }
        let n = sc.buf.len();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, 0);
        let (buf, table) = (&sc.buf, &mut sc.table);
        for g in 0..n {
            let e = unsafe { buf.get_unchecked(g) };
            let identity=tokens.identity(e.eid());
            let hash=eid_hash(identity);
            let fp=hash>>32;
            let mut s=hash as usize&mask;
            loop {
                let t = unsafe { *table.get_unchecked(s) };
                if t == 0 {
                    unsafe { *table.get_unchecked_mut(s) = (fp << 32) | (g as u64 + 1) };
                    break;
                }
                if t >> 32 == fp {
                    let j = (t & 0xFFFF_FFFF) as usize - 1;
                    let old = unsafe { buf.get_unchecked(j) };
                    if tokens.identity(old.eid()) == identity {
                        // equal ingest_ts means identical copies (rules.md): keep the first
                        if tokens.seconds(e.eid(),e.ingest()) < tokens.seconds(old.eid(),old.ingest()) {
                            unsafe { *table.get_unchecked_mut(s) = (fp << 32) | (g as u64 + 1) };
                            if old.loc() != NOLOC {
                                dead.kill(old.loc(),tokens);
                            }
                        } else if e.loc() != NOLOC {
                            dead.kill(e.loc(),tokens);
                        }
                        break;
                    }
                }
                s = (s + 1) & mask;
            }
        }
        span.finish(n,n*std::mem::size_of::<CopyR>());
    })
}

/// The live event records of person partition `b`, appended to `out`.
pub fn live_events(b: usize, sinks: &[Sink], dead: &Dead, out: &mut Vec<Ev2>) {
    out.clear();
    let n: usize = sinks.iter().map(|s| s.ev.len(b)).sum();
    out.reserve(n);
    for (t, s) in sinks.iter().enumerate() {
        let mut i = 0usize;
        for sl in s.ev.slices(b) {
            // records were streamed out of the cache: fetch ahead (the hardware
            // prefetcher restarts at every 4 KiB)
            for (j, e) in sl.iter().enumerate() {
                if j % 4 == 0 {
                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        std::arch::x86_64::_mm_prefetch(sl.as_ptr().wrapping_add(j + 24) as *const i8, std::arch::x86_64::_MM_HINT_T0)
                    };
                }
                if (dead.word(t, b, i / 64) >> (i % 64)) & 1 == 0 {
                    unsafe {
                        let len = out.len();
                        out.as_mut_ptr().add(len).write(*e);
                        out.set_len(len + 1);
                    }
                }
                i += 1;
            }
        }
    }
}

pub fn clicks_of(b: usize, sinks: &[Sink], out: &mut Vec<Ck>) {
    out.clear();
    for s in sinks {
        for sl in s.ck.slices(b) {
            out.extend_from_slice(sl);
        }
    }
}

#[cfg(test)]
mod copy_layout_tests {
    use super::*;
    #[test]
    fn copy_fields_do_not_overlap() {
        assert_eq!(std::mem::size_of::<CopyR>(),16);
        assert_eq!(std::mem::size_of::<Ev2>(),16);
        for eid in [0,1,(1<<47)-1,(1<<48)-1] {
            for time in [0,1,65535,65536,u32::MAX] {
                for loc in [0,1,(1<<47)-1,(1<<48)-1] {
                    let r=CopyR::new(eid,time,loc);
                    assert_eq!((r.eid(),r.ingest(),r.loc()),(eid,time,loc));
                }
            }
        }
    }
}

/// The dense path shares all validation rules with the ordinary row handler.
fn checked_event(line:&'static [u8],fs:&crate::scan::Flds<7>,lk:&Lookups,ws:i64,we:i64,ingest:i64,memo:&mut parse::Memo)->Option<(&'static [u8],u32,u32)> {
    let user=fs.get(line,1);if user.is_empty() {return None;}
    let ts=parse::ts_memo(fs.get(line,2),memo)?;
    if ts<ws||ts>=we||ingest-ts>DAY_MS||fs.get(line,5).is_empty() {return None;}
    let kind=fs.get(line,4);
    let pc=if kind==b"view"||kind==b"cart" {0}
        else if kind==b"purchase" {
            let price=parse::price(fs.get(line,6))?;
            let category=lk.products.get(h64(fs.get(line,5))).unwrap_or(lk.unknown_cat as u64) as u32;
            (price<<8)|category
        } else {return None;};
    Some((user,(ts-ws) as u32,pc))
}

#[allow(clippy::too_many_arguments)]
pub fn events_unit_dense(f:usize,s:usize,e:usize,lk:&Lookups,persons:&Persons,tokens:&crate::event_token::Tokens,dense:&crate::dense_events::Context,ws:i64,we:i64,sink:&mut Sink,tid:u64) {
    let users=persons.map.dense.as_ref().unwrap();let mut memo=parse::Memo::new();
    crate::csv_local::records(f,s,e,|_,line,fields| {
        let Some(fields)=fields else {return;};let id=fields.get(line,0);if id.is_empty() {return;}
        let number=dense.model.event(id);
        let predicted=if dense.defer_users {None} else {let index=users.model.user(fields.get(line,1));users.prefetch_index(index);index};
        let ingest_ms=parse::ingest_memo(fields.get(line,3),&mut memo).unwrap_or(i64::MAX/2);
        let ingest=ingest_ms.div_euclid(1000);
        let valid=checked_event(line,fields,lk,ws,we,ingest_ms,&mut memo);
        if let Some(number)=number {
            let (code,ts,pc,payload)=if let Some((user,ts,pc))=valid {
                let code=if dense.defer_users {
                    if dense.inline_users {0} else {
                    let index=sink.deferred_users.len();
                    if index>=1usize<<dense.user_index_bits {dense.failed.store(true,Ordering::Relaxed);return;}
                    sink.deferred_users.push(ids::payload_of(user));
                    ((tid as u32)<<dense.user_index_bits)|index as u32}
                } else {let Some(code)=users.code_index(predicted,user) else {dense.failed.store(true,Ordering::Relaxed);return;};code};
                (code,ts,pc,if dense.inline_users {ids::payload_of(user)} else {0})
            } else {(0,u32::MAX,0,0)};
            let part=(number>>dense.width) as usize;let record=dense.record(number,ingest,code,ts,pc);
            if dense.inline_users {sink.append_inline(part,crate::dense_events::InlineRecord{record,payload});}
            else {sink.append_dense(part,record);}
        } else {
            // The same spelling always uses the same class; non-canonical and
            // out-of-range labels are deduplicated by the existing exact path.
            let (eid,compact,identity)=tokens.stamp(id,ingest);
            let loc=if let Some((user,ts,pc))=valid {
                let key=users.person_index(predicted,user);let part=person_bucket(key);
                let i=sink.ev.push(&mut sink.arena,part,Ev2{key,ts,pc});
                tokens.location(tid as usize,part,i as usize)
            } else {NOLOC};
            sink.cp.push(&mut sink.arena,eid_part(identity),CopyR::new(eid,compact,loc));
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub fn clicks_unit_dense(f:usize,s:usize,e:usize,lk:&Lookups,persons:&Persons,ws:i64,we:i64,sink:&mut Sink) {
    let users=persons.map.dense.as_ref().unwrap();let mut memo=parse::Memo::new();let lo=ws-CLICK_REACH;
    crate::scan::scan_records::<4>(f,s,e,|p,line,fields| {
        let Some(fields)=fields else {return;};let id=fields.get(line,0);let user=fields.get(line,1);
        if id.is_empty()||user.is_empty() {return;}
        let predicted=users.model.user(user);users.prefetch_index(predicted);
        let Some(ts)=parse::ts_memo(fields.get(line,3),&mut memo) else {return;};
        if ts<lo||ts>=we {return;}
        let Some(camp)=lk.campaigns.get(h64(fields.get(line,2))) else {return;};
        let key=users.person_index(predicted,user);
        sink.ck.push(&mut sink.arena,person_bucket(key),Ck{key,cref:mkref(f,p+fields.span(line,0).0,id.len()),ts:(ts-lo) as u32,camp:camp as u32});
    });
}
