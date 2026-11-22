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

/// One copy of an event, valid or not, for dedup. 16 bytes.
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct CopyR {
    /// event id hash with its top PD bits (fixed by the partition) replaced by
    /// check bits: 64 + PD bits of identity
    id: u64,
    ingest: u32,
    /// person partition << LOC_I | index in this worker's stream of it; NOLOC
    /// when invalid. The worker is the stream's owner.
    loc: u32,
}

const NOLOC: u32 = u32::MAX;
const LOC_I: u32 = 20;

/// A copy whose event record index does not fit `loc` (a person partition
/// holding over a million of one worker's events): kept aside, same rules.
#[derive(Clone, Copy)]
pub struct Wide {
    part: u32,
    tid: u32,
    id: u64,
    ingest: u32,
    pb: u32,
    i: u32,
}

#[inline(always)]
fn copy_id(h: u64, c: u32) -> u64 {
    let b = pd_bits();
    (h & (u64::MAX >> b)) | (((c as u64) >> (32 - b)) << (64 - b))
}

#[inline(always)]
fn eid_part(eid: u64) -> usize {
    (eid >> (64 - pd_bits())) as usize
}

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
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Line([u8; 64]);

pub struct Stream<T> {
    tails: Vec<Tail>,
    blocks: Vec<Vec<usize>>,
    /// K records per partition, cache-line aligned (a row is whole lines)
    stage: Vec<Line>,
    wc: bool,
    _t: PhantomData<T>,
}

impl<T: Copy + Default> Stream<T> {
    /// Records per staging flush: a whole number of cache lines.
    const K: usize = match std::mem::size_of::<T>() {
        12 => 16,
        16 => 4,
        24 => 8,
        32 => 4,
        _ => 1,
    };
    const ROW_LINES: usize = Self::K * std::mem::size_of::<T>() / 64;
    /// Records per block, a multiple of K.
    pub const CAP: usize = BLOCK_BYTES / std::mem::size_of::<T>() / Self::K * Self::K;

    fn new(parts: usize) -> Self {
        let wc = WC.load(Ordering::Relaxed) && Self::K > 1;
        let stage = if wc { vec![Line([0; 64]); parts * Self::ROW_LINES] } else { Vec::new() };
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
        unsafe { (self.stage.as_mut_ptr().add(p * Self::ROW_LINES) as *mut T).add(f).write(v) };
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
            let src = self.stage.as_ptr().add(p * Self::ROW_LINES) as *const u8;
            let dst = t.cur as *mut u8;
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                use std::arch::x86_64::*;
                let mut o = 0;
                while o < bytes {
                    let x = _mm256_load_si256(src.add(o) as *const __m256i);
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
                std::ptr::copy_nonoverlapping(self.stage.as_ptr().add(p * Self::ROW_LINES) as *const T, t.cur as *mut T, rest);
            }
            t.cur += rest * std::mem::size_of::<T>();
            t.left -= rest as u32;
        }
    }

    #[inline]
    pub fn len(&self, p: usize) -> usize {
        self.tails[p].n as usize
    }

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
    /// IDs of unlinked users, copied while they are in cache: names never read
    /// the event files' mapping (no page faults, nothing mapped to tear down at
    /// exit). Refs into it use file number `name_file`.
    pub names: Vec<u8>,
    #[allow(dead_code)]
    name_file: usize,
    pub wide: Vec<Wide>,
    /// this worker's block of person ids for new users
    pub pids: (u32, u32),
}

// raw block pointers into this sink's own arena
unsafe impl Send for Sink {}
unsafe impl Sync for Sink {}

impl Sink {
    pub fn new(name_file: usize, name_cap: usize) -> Self {
        Sink {
            arena: Arena::new(),
            ev: Stream::new(p2()),
            cp: Stream::new(pd()),
            ck: Stream::new(p2()),
            names: Vec::with_capacity(name_cap + 16),
            name_file,
            wide: Vec::new(),
            pids: (0, 0),
        }
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
    uref: u64,
    /// copy record id (copy_id) and its event id partition
    id: u64,
    part: u32,
    ingest: u32,
    ts: u32,
    pc: u32,
}

#[inline(always)]
fn emit_event(sink: &mut Sink, persons: &Persons, tid: u64, pd: &PendE) {
    let pid = persons.pid(pd.uh, pd.uref, &mut sink.pids);
    let pb = (pid as usize) & (p2() - 1);
    let i = sink.ev.push(&mut sink.arena, pb, Ev2 { pid, ts: pd.ts, pc: pd.pc });
    let (part, id) = (pd.part as usize, pd.id);
    if i < (1 << LOC_I) - 1 && pb < 1 << (32 - LOC_I) {
        sink.cp.push(&mut sink.arena, part, CopyR { id, ingest: pd.ingest, loc: ((pb as u32) << LOC_I) | i });
    } else {
        sink.wide.push(Wide { part: part as u32, tid: tid as u32, id, ingest: pd.ingest, pb: pb as u32, i });
    }
}

/// Parse one unit of event lines into `sink`.
#[allow(clippy::too_many_arguments)]
pub fn events_unit(f: usize, s: usize, e: usize, lk: &Lookups, persons: &Persons, ws: i64, we: i64, sink: &mut Sink, tid: u64) {
    const AHEAD: usize = 32;
    let mut ring = [PendE::default(); AHEAD];
    let mut memo = parse::Memo::new();
    let mut head = 0usize;
    let mut filled = 0usize;
    crate::scan::scan_lines::<7>(f, s, e, |p, line, fs| {
        let Some(fs) = fs else { return };
        let fld = |i: usize| fs.get(line, i);
        let eid = fld(0);
        if eid.is_empty() {
            return;
        }
        let (ingest_ms, ingest) = parse::ingest_pair(fld(3), &mut memo);
        let (eid_h, eid_c) = h64_chk(eid);
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
            persons.prefetch(uh);
            if filled == AHEAD {
                emit_event(sink, persons, tid, &ring[head]);
            } else {
                filled += 1;
            }
            // built in place in the ring slot just emitted
            let pd = &mut ring[head];
            let ua = user.as_ptr() as usize - line.as_ptr() as usize;
            pd.uh = uh;
            pd.uref = mkref(f, p + ua, user.len());
            pd.id = copy_id(eid_h, eid_c);
            pd.part = eid_part(eid_h) as u32;
            pd.ingest = ingest;
            pd.ts = (ts - ws) as u32;
            pd.pc = pc;
            head = (head + 1) % AHEAD;
            return;
        }
        // an invalid copy still takes part in dedup
        sink.cp.push(&mut sink.arena, eid_part(eid_h), CopyR { id: copy_id(eid_h, eid_c), ingest, loc: NOLOC });
    });
    let start = if filled == AHEAD { head } else { 0 };
    for k in 0..filled {
        let pd = ring[(start + k) % AHEAD];
        emit_event(sink, persons, tid, &pd);
    }
}

#[derive(Clone, Copy, Default)]
struct PendC {
    uh: u64,
    /// the user ID (names the person if this click meets the ID first)
    uref: u64,
    cref: u64,
    ts: u32,
    camp: u32,
}

#[inline(always)]
fn emit_click(sink: &mut Sink, persons: &Persons, pd: &PendC) {
    let pid = persons.pid(pd.uh, pd.uref, &mut sink.pids);
    sink.ck.push(&mut sink.arena, (pid as usize) & (p2() - 1), Ck { pid, cref: pd.cref, ts: pd.ts, camp: pd.camp });
}

/// Parse one unit of click lines into `sink`.
#[allow(clippy::too_many_arguments)]
pub fn clicks_unit(f: usize, s: usize, e: usize, lk: &Lookups, persons: &Persons, ws: i64, we: i64, sink: &mut Sink) {
    const AHEAD: usize = 32;
    let lo = ws - CLICK_REACH;
    let mut ring = [PendC::default(); AHEAD];
    let mut memo = parse::Memo::new();
    let mut head = 0usize;
    let mut filled = 0usize;
    crate::scan::scan_lines::<4>(f, s, e, |p, line, fs| {
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
        persons.prefetch(uh);
        let pd = PendC { uh, uref: mkref(f, p + fs.span(line, 1).0, user.len()), cref: mkref(f, p + fs.span(line, 0).0, cid.len()), ts: (ts - lo) as u32, camp: camp as u32 };
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
    fn kill(&self, t: usize, p: usize, i: usize) {
        self.words[self.off[t * self.np + p] + i / 64].fetch_or(1 << (i % 64), Ordering::Relaxed);
    }
    #[inline(always)]
    pub fn word(&self, t: usize, p: usize, w: usize) -> u64 {
        unsafe { self.words.get_unchecked(*self.off.get_unchecked(t * self.np + p) + w) }.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct DedupScratch2 {
    blocks: Vec<(usize, u32, u32)>, // (records, count, owning worker)
    table: Vec<u64>,
}

thread_local! {
    static DEDUP2: std::cell::RefCell<DedupScratch2> = std::cell::RefCell::new(DedupScratch2::default());
}

/// The wide copies of all workers, sorted by event id partition.
pub fn wide_sorted(sinks: &mut [Sink]) -> Vec<Wide> {
    let mut w: Vec<Wide> = sinks.iter_mut().flat_map(|s| std::mem::take(&mut s.wide)).collect();
    w.sort_by_key(|x| x.part);
    w
}

/// Keep the earliest-ingested copy of every event id in partition `b`; mark
/// the event records of all other copies dead.
pub fn dedup_part(b: usize, sinks: &[Sink], wide: &[Wide], dead: &Dead) {
    DEDUP2.with(|cell| {
        let mut guard = cell.borrow_mut();
        let sc = &mut *guard;
        // the copy records stay in their blocks: a table entry names a copy by
        // block << 10 | offset (a block holds at most 512), or WIDE | index
        const OB: u32 = 10;
        const WIDE: usize = 1 << 31;

        sc.blocks.clear();
        let mut n = 0usize;
        for (t, s) in sinks.iter().enumerate() {
            for sl in s.cp.slices(b) {
                n += sl.len();
                sc.blocks.push((sl.as_ptr() as usize, sl.len() as u32, t as u32));
            }
        }
        let wide = if wide.is_empty() {
            wide
        } else {
            let lo = wide.partition_point(|x| (x.part as usize) < b);
            let hi = wide.partition_point(|x| (x.part as usize) <= b);
            &wide[lo..hi]
        };
        n += wide.len();
        assert!(sc.blocks.len() < WIDE >> OB, "dedup partition too large");
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, 0);
        let (blocks, table) = (&sc.blocks, &mut sc.table);
        // (id, ingest, event record to kill: worker, partition, index)
        let copy = |g: usize| -> (u64, u32, Option<(usize, usize, usize)>) {
            if g & WIDE != 0 {
                let w = unsafe { wide.get_unchecked(g & !WIDE) };
                (w.id, w.ingest, Some((w.tid as usize, w.pb as usize, w.i as usize)))
            } else {
                let (p, _, t) = unsafe { *blocks.get_unchecked(g >> OB) };
                let r = unsafe { &*(p as *const CopyR).add(g & ((1 << OB) - 1)) };
                let at = if r.loc == NOLOC { None } else { Some((t as usize, (r.loc >> LOC_I) as usize, (r.loc & ((1 << LOC_I) - 1)) as usize)) };
                (r.id, r.ingest, at)
            }
        };
        let mut one = |g: usize, id: u64, ingest: u32| {
            let fp = id >> 32;
            let mut s = (id as usize) & mask;
            loop {
                let t = unsafe { *table.get_unchecked(s) };
                if t == 0 {
                    unsafe { *table.get_unchecked_mut(s) = (fp << 32) | (g as u64 + 1) };
                    return;
                }
                if t >> 32 == fp {
                    let j = (t & 0xFFFF_FFFF) as usize - 1;
                    let (oid, oingest, oat) = copy(j);
                    if oid == id {
                        // equal ingest_ts means identical copies (rules.md): keep the first
                        let lose = if ingest < oingest {
                            unsafe { *table.get_unchecked_mut(s) = (fp << 32) | (g as u64 + 1) };
                            oat
                        } else {
                            copy(g).2
                        };
                        if let Some((t, p, i)) = lose {
                            dead.kill(t, p, i);
                        }
                        return;
                    }
                }
                s = (s + 1) & mask;
            }
        };
        for (bi, &(p, len, _)) in blocks.iter().enumerate() {
            let blk = unsafe { std::slice::from_raw_parts(p as *const CopyR, len as usize) };
            for (o, e) in blk.iter().enumerate() {
                one((bi << OB) | o, e.id, e.ingest);
            }
        }
        for (k, w) in wide.iter().enumerate() {
            one(WIDE | k, w.id, w.ingest);
        }
    })
}
const _: () = assert!(Stream::<CopyR>::CAP <= 1024, "dedup names a copy by block << 10 | offset");
const _: () = assert!(std::mem::size_of::<CopyR>() == 16);
