//! A minimal Parquet writer: one row group, one PLAIN-encoded uncompressed
//! v1 data page per column, Thrift compact metadata. Covers exactly what the
//! two output tables need: required INT32 / DATE / INT64 / UTF8 columns and an
//! optional UTF8 column.

use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    I32,
    Date,
    I64,
    Str,
    OptStr,
}

/// One column's PLAIN page body, built row by row.
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    body: Vec<u8>,
    /// definition levels (OptStr only), one bit per row, LSB first
    defs: Vec<u8>,
    rows: usize,
    /// dictionary-encoded strings (`str_run`): `body` is the dictionary
    /// (PLAIN), these are the rows' indices into it
    idx: Vec<u32>,
    dict_n: u32,
    last: u64,
}

/// SS_DICT=0: every column PLAIN (default: dictionary pages for the user IDs
/// and the small-range INT32 columns, about half the bytes to write).
pub static DICT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

#[inline]
fn dict_on() -> bool {
    DICT.load(std::sync::atomic::Ordering::Relaxed)
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, rows: usize, bytes: usize) -> Self {
        Column { name, kind, body: Vec::with_capacity(bytes), defs: Vec::with_capacity(rows / 8 + 1), rows: 0, idx: Vec::new(), dict_n: 0, last: u64::MAX }
    }
    /// A string column built for dictionary encoding when enabled: rows with
    /// the same `key` in a row share one dictionary entry.
    pub fn new_dict(name: &'static str, rows: usize, bytes: usize) -> Self {
        let mut c = Column::new(name, Kind::Str, rows, bytes);
        if dict_on() {
            c.idx = Vec::with_capacity(rows);
        }
        c
    }
    /// One row of a `new_dict` column: `key` names the string `v` (equal keys
    /// have equal strings).
    #[inline(always)]
    pub fn str_run(&mut self, key: u64, v: impl FnOnce() -> &'static [u8]) {
        if self.idx.capacity() == 0 {
            self.str(v());
            return;
        }
        if key != self.last || self.dict_n == 0 {
            let v = v();
            self.body.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.body.extend_from_slice(v);
            self.dict_n += 1;
            self.last = key;
        }
        self.idx.push(self.dict_n - 1);
        self.rows += 1;
    }
    #[inline(always)]
    pub fn i32(&mut self, v: i32) {
        self.body.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
    #[inline(always)]
    pub fn i64(&mut self, v: i64) {
        self.body.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
    #[inline(always)]
    pub fn str(&mut self, v: &[u8]) {
        self.body.extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.body.extend_from_slice(v);
        self.rows += 1;
    }
    #[inline(always)]
    fn def(&mut self, present: bool) {
        let r = self.rows;
        if r % 8 == 0 {
            self.defs.push(0);
        }
        if present {
            *self.defs.last_mut().unwrap() |= 1 << (r % 8);
        }
    }
    #[inline(always)]
    pub fn opt_str(&mut self, v: Option<&[u8]>) {
        self.def(v.is_some());
        if let Some(v) = v {
            self.body.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.body.extend_from_slice(v);
        }
        self.rows += 1;
    }
    fn ptype(&self) -> i32 {
        match self.kind {
            Kind::I32 | Kind::Date => 1,
            Kind::I64 => 2,
            Kind::Str | Kind::OptStr => 6,
        }
    }
}

// ---------------------------------------------------------------- thrift compact

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                self.b.push(byte);
                return;
            }
            self.b.push(byte | 0x80);
        }
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            let z = ((id as i64) << 1) ^ ((id as i64) >> 63);
            self.varint(z as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, 8);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// A struct element of a list.
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
}

fn page_header(num_values: usize, size: usize, enc: i32) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    t.begin(5); // DataPageHeader
    t.i32(1, num_values as i32);
    t.i32(2, enc); // PLAIN or RLE_DICTIONARY
    t.i32(3, 3); // RLE definition levels
    t.i32(4, 3); // RLE repetition levels
    t.end();
    t.b.push(0);
    t.b
}

fn dict_page_header(num_values: usize, size: usize) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 2); // DICTIONARY_PAGE
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    t.begin(7); // DictionaryPageHeader
    t.i32(1, num_values as i32);
    t.i32(2, 0); // PLAIN
    t.end();
    t.b.push(0);
    t.b
}

/// Bit width byte + one bit-packed run (RLE/bit-packing hybrid) of `idx`,
/// each below 2^w.
fn bitpack(idx: impl ExactSizeIterator<Item = u32>, w: u32) -> Vec<u8> {
    let n = idx.len();
    let groups = n.div_ceil(8);
    let mut out = Vec::with_capacity(groups * w as usize + 16);
    out.push(w as u8);
    let mut t = T::new();
    t.varint(((groups as u64) << 1) | 1);
    out.extend_from_slice(&t.b);
    let mut acc: u64 = 0;
    let mut bits = 0u32;
    for v in idx {
        acc |= (v as u64) << bits;
        bits += w;
        while bits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    // pad the last group to 8 values
    let pad_bits = (groups * 8 - n) as u32 * w + bits;
    let mut left = pad_bits;
    while left > 0 {
        out.push(acc as u8);
        acc >>= 8;
        left = left.saturating_sub(8);
    }
    debug_assert_eq!(out.len(), 1 + t.b.len() + groups * w as usize);
    out
}

#[inline]
fn width(n: u32) -> u32 {
    // bits for indices 0..n (at least 1)
    (32 - n.saturating_sub(1).leading_zeros()).max(1)
}

/// One column chunk laid out: its pieces in file order and where its pages are.
struct Chunk<'a> {
    pieces: Vec<std::borrow::Cow<'a, [u8]>>,
    dict: bool,
    size: usize,
}

fn chunk_of(c: &Column, rows: usize) -> Chunk<'_> {
    use std::borrow::Cow;
    let mut pieces: Vec<Cow<[u8]>> = Vec::new();
    // dictionary-encoded strings
    if c.idx.capacity() > 0 && c.kind == Kind::Str && c.dict_n > 0 {
        pieces.push(Cow::Owned(dict_page_header(c.dict_n as usize, c.body.len())));
        pieces.push(Cow::Borrowed(&c.body[..]));
        let data = bitpack(c.idx.iter().copied(), width(c.dict_n));
        pieces.push(Cow::Owned(page_header(rows, data.len(), 8)));
        pieces.push(Cow::Owned(data));
        let size = pieces.iter().map(|p| p.len()).sum();
        return Chunk { pieces, dict: true, size };
    }
    // small-range INT32: the dictionary is the range lo..=hi
    if dict_on() && matches!(c.kind, Kind::I32 | Kind::Date) && rows > 0 {
        let vals = c.body.chunks_exact(4).map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]));
        let (lo, hi) = vals.clone().fold((i32::MAX, i32::MIN), |(a, b), v| (a.min(v), b.max(v)));
        let range = hi as i64 - lo as i64 + 1;
        if range <= 4096 && (range as usize) * 4 < c.body.len() / 2 {
            let mut d = Vec::with_capacity(range as usize * 4);
            for v in lo..=hi {
                d.extend_from_slice(&v.to_le_bytes());
            }
            pieces.push(Cow::Owned(dict_page_header(range as usize, d.len())));
            pieces.push(Cow::Owned(d));
            let data = bitpack(vals.map(|v| (v - lo) as u32), width(range as u32));
            pieces.push(Cow::Owned(page_header(rows, data.len(), 8)));
            pieces.push(Cow::Owned(data));
            let size = pieces.iter().map(|p| p.len()).sum();
            return Chunk { pieces, dict: true, size };
        }
    }
    // PLAIN
    let levels: Vec<u8> = if c.kind == Kind::OptStr {
        // RLE/bit-packed hybrid, one bit-packed run of ceil(rows / 8) groups
        let mut lv = Vec::with_capacity(c.defs.len() + 8);
        let mut t = T::new();
        t.varint(((c.defs.len() as u64) << 1) | 1);
        lv.extend_from_slice(&t.b);
        lv.extend_from_slice(&c.defs);
        lv
    } else {
        Vec::new()
    };
    let mut pre: Vec<u8> = Vec::new();
    if c.kind == Kind::OptStr {
        pre.extend_from_slice(&(levels.len() as u32).to_le_bytes());
        pre.extend_from_slice(&levels);
    }
    let size = pre.len() + c.body.len();
    let mut hdr = page_header(rows, size, 0);
    hdr.extend_from_slice(&pre);
    pieces.push(Cow::Owned(hdr));
    pieces.push(Cow::Borrowed(&c.body[..]));
    let size = pieces.iter().map(|p| p.len()).sum();
    Chunk { pieces, dict: false, size }
}

/// All of `bufs` to `f` with as few system calls as possible.
fn write_all_vectored(f: &mut std::fs::File, bufs: &[&[u8]]) {
    let mut v: Vec<std::io::IoSlice> = bufs.iter().filter(|b| !b.is_empty()).map(|b| std::io::IoSlice::new(b)).collect();
    let mut s = &mut v[..];
    while !s.is_empty() {
        let n = f.write_vectored(s).expect("write parquet");
        assert!(n > 0, "write parquet: no progress");
        std::io::IoSlice::advance_slices(&mut s, n);
    }
}

/// Write `cols` (all of the same row count) as one Parquet file.
pub fn write_file(path: &Path, cols: &[Column]) {
    let rows = cols.first().map_or(0, |c| c.rows);
    let mut layout: Vec<Chunk> = Vec::new();
    // (offset of the first page, offset of the data page, total size)
    let mut at: Vec<(usize, usize, usize)> = Vec::new();
    let mut off = 4usize; // after "PAR1"
    for c in cols.iter().filter(|_| rows > 0) {
        assert_eq!(c.rows, rows, "column {} has {} rows, not {rows}", c.name, c.rows);
        let ch = chunk_of(c, rows);
        let data_off = if ch.dict { off + ch.pieces[0].len() + ch.pieces[1].len() } else { off };
        at.push((off, data_off, ch.size));
        off += ch.size;
        layout.push(ch);
    }
    let chunks: Vec<(usize, usize)> = at.iter().map(|&(o, _, s)| (o, s)).collect();
    // FileMetaData
    let mut t = T::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    t.elem_begin();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.elem_begin();
        t.i32(1, c.ptype());
        t.i32(3, if c.kind == Kind::OptStr { 1 } else { 0 });
        t.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Str | Kind::OptStr => {
                t.i32(6, 0); // UTF8
                t.begin(10);
                t.begin(1); // StringType
                t.end();
                t.end();
            }
            Kind::Date => {
                t.i32(6, 6); // DATE
                t.begin(10);
                t.begin(6); // DateType
                t.end();
                t.end();
            }
            _ => {}
        }
        t.end();
    }
    t.i64(3, rows as i64);
    if rows == 0 {
        // no row group at all for an empty table
        t.list(4, 12, 0);
        t.bin(6, b"ssp");
        t.b.push(0);
        let mut out: Vec<u8> = b"PAR1".to_vec();
        out.extend_from_slice(&t.b);
        out.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
        out.extend_from_slice(b"PAR1");
        std::fs::File::create(path).expect("create parquet").write_all(&out).expect("write parquet");
        return;
    }
    t.list(4, 12, 1);
    t.elem_begin();
    t.list(1, 12, cols.len());
    let total: usize = chunks.iter().map(|c| c.1).sum();
    for ((c, &(off, size)), (&(_, data_off, _), ch)) in cols.iter().zip(&chunks).zip(at.iter().zip(&layout)) {
        t.elem_begin();
        t.i64(2, off as i64);
        t.begin(3);
        t.i32(1, c.ptype());
        if ch.dict {
            t.list(2, 5, 3);
            t.zigzag(0);
            t.zigzag(3);
            t.zigzag(8);
        } else if c.kind == Kind::OptStr {
            t.list(2, 5, 2);
            t.zigzag(0);
            t.zigzag(3);
        } else {
            t.list(2, 5, 1);
            t.zigzag(0);
        }
        t.list(3, 8, 1);
        t.varint(c.name.len() as u64);
        t.b.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, rows as i64);
        t.i64(6, size as i64);
        t.i64(7, size as i64);
        t.i64(9, data_off as i64);
        if ch.dict {
            t.i64(11, off as i64);
        }
        t.end();
        t.end();
    }
    t.i64(2, total as i64);
    t.i64(3, rows as i64);
    t.end();
    t.bin(6, b"ssp");
    t.b.push(0);
    let tail_len = (t.b.len() as u32).to_le_bytes();
    let mut bufs: Vec<&[u8]> = vec![b"PAR1"];
    for ch in &layout {
        for p in &ch.pieces {
            bufs.push(p);
        }
    }
    bufs.push(&t.b);
    bufs.push(&tail_len);
    bufs.push(b"PAR1");
    let mut f = std::fs::File::create(path).expect("create parquet");
    write_all_vectored(&mut f, &bufs);
}
