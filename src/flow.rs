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
}

// raw block pointers into this sink's own arena
unsafe impl Send for Sink {}
unsafe impl Sync for Sink {}

impl Sink {
    pub fn new() -> Self {
        Sink { arena: Arena::new(), ev: Stream::new(p2()), cp: Stream::new(pd()), ck: Stream::new(p2()) }
    }
    /// End of the parse phase: everything staged goes to its blocks.
    pub fn finish(&mut self) {
        self.ev.finish(&mut self.arena);
        self.cp.finish(&mut self.arena);
        self.ck.finish(&mut self.arena);
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
    crate::scan::scan_records::<7>(f, s, e, |p, line, fs| {
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
                    if tokens.same_id(old.eid(),e.eid()) {
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

#[cfg(test)]
mod byte_ref_tests {
    use super::*;
    #[test]
    fn colliding_hashes_keep_distinct_ids_and_earliest_invalid_copy_wins() {
        let blob:&'static [u8]=b"opaque-event-a|opaque-event-b|opaque-event-a|opaque-event-c";
        let tokens=crate::event_token::Tokens::test_refs(blob,Some(123));
        let a=&blob[..14];let b=&blob[15..29];let again=&blob[30..44];let c=&blob[45..];
        let records=[(a,9,true),(b,8,true),(again,-1,true),(b"e0".as_slice(),1,true),(a,-2,false),(b,7,true),(c,u32::MAX as i64+1,true),(c,u32::MAX as i64+2,true)];
        let mut sink=Sink::new();
        for (id,seconds,valid) in records {
            let (stored,seconds,hash)=tokens.stamp(id,seconds);assert_eq!(hash,123);
            let loc=if valid {
                let i=sink.ev.push(&mut sink.arena,0,Ev2{key:1,ts:0,pc:0});tokens.location(0,0,i as usize)
            } else {NOLOC};
            sink.cp.push(&mut sink.arena,0,CopyR::new(stored,seconds,loc));
        }
        sink.finish();
        #[cfg(target_arch="x86_64")] unsafe {std::arch::x86_64::_mm_sfence();}
        let sinks=[sink];let dead=Dead::new(&sinks);
        dedup_part(0,&sinks,&dead,&tokens);
        assert_eq!(dead.word(0,0,0),0b1000111);
    }
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
