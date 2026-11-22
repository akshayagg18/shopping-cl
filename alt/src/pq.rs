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
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, rows: usize, bytes: usize) -> Self {
        Column { name, kind, body: Vec::with_capacity(bytes), defs: Vec::with_capacity(rows / 8 + 1), rows: 0 }
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

fn page_header(num_values: usize, size: usize) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    t.begin(5); // DataPageHeader
    t.i32(1, num_values as i32);
    t.i32(2, 0); // PLAIN
    t.i32(3, 3); // RLE definition levels
    t.i32(4, 3); // RLE repetition levels
    t.end();
    t.b.push(0);
    t.b
}

/// Write `cols` (all of the same row count) as one Parquet file.
pub fn write_file(path: &Path, cols: &[Column]) {
    let rows = cols.first().map_or(0, |c| c.rows);
    let mut out: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.body.len() + c.defs.len() + 64).sum::<usize>() + 4096);
    out.extend_from_slice(b"PAR1");
    let mut chunks: Vec<(usize, usize)> = Vec::new(); // (offset, total size)
    for c in cols.iter().filter(|_| rows > 0) {
        assert_eq!(c.rows, rows, "column {} has {} rows, not {rows}", c.name, c.rows);
        let mut body: Vec<u8> = Vec::new();
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
        if c.kind == Kind::OptStr {
            body.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            body.extend_from_slice(&levels);
        }
        let size = body.len() + c.body.len();
        let hdr = page_header(rows, size);
        let off = out.len();
        out.extend_from_slice(&hdr);
        out.extend_from_slice(&body);
        out.extend_from_slice(&c.body);
        chunks.push((off, out.len() - off));
    }
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
    for (c, &(off, size)) in cols.iter().zip(&chunks) {
        t.elem_begin();
        t.i64(2, off as i64);
        t.begin(3);
        t.i32(1, c.ptype());
        if c.kind == Kind::OptStr {
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
        t.i64(9, off as i64);
        t.end();
        t.end();
    }
    t.i64(2, total as i64);
    t.i64(3, rows as i64);
    t.end();
    t.bin(6, b"ssp");
    t.b.push(0);
    out.extend_from_slice(&t.b);
    out.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path).expect("create parquet");
    f.write_all(&out).expect("write parquet");
}
