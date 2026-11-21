//! Parquet writing: one user_daily file per aggregation state, written in parallel.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_array::builder::StringBuilder;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use rayon::prelude::*;

use crate::{Agg, Lookups, OutInfo, name_bytes};

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_data_page_size_limit(8 << 20)
        .set_max_row_group_row_count(Some(1 << 24))
        .set_write_batch_size(1 << 16)
        .build()
}

fn write_batch(path: &Path, batch: RecordBatch) {
    let f = BufWriter::with_capacity(4 << 20, File::create(path).expect("create parquet"));
    let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props())).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
}

fn user_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn utf8(b: &[u8]) -> &str {
    // Inputs are UTF-8 by contract; avoid a validation pass on the hot path.
    unsafe { std::str::from_utf8_unchecked(b) }
}

pub fn write(out: &Path, aggs: Vec<Agg>, lk: &Lookups, info: &OutInfo) {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    // merge campaign arrays (small) into [camp * ndays + day] order
    let w = info.ncamp + 1;
    let mut camp = vec![[0i64; 4]; w * info.ndays];
    for a in &aggs {
        for d in 0..info.ndays {
            for c in 0..w {
                let t = &mut camp[c * info.ndays + d];
                let cr = a.credit[d * w + c];
                t[0] += cr[0];
                t[1] += cr[1];
                t[2] += cr[2];
                t[3] += a.assist[d * w + c] as i64;
            }
        }
    }

    let cats = &lk.categories;
    let schema = user_schema();
    let campaign_job = || {
        let (mut id, mut ch, mut day) = (StringBuilder::new(), StringBuilder::new(), Vec::new());
        let (mut s, mut p, mut r, mut a) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..=info.ncamp {
            for d in 0..info.ndays {
                let v = camp[c * info.ndays + d];
                if v[0] == 0 && v[3] == 0 {
                    continue;
                }
                if c == info.ncamp {
                    id.append_value("organic");
                    ch.append_value("organic");
                } else {
                    id.append_value(utf8(&lk.campaign_names[c]));
                    ch.append_value(utf8(&lk.channels[c]));
                }
                day.push((info.ws_day + d as i64) as i32);
                s.push(v[0]);
                p.push(v[1]);
                r.push(v[2]);
                a.push(v[3]);
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("purchasing_sessions", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("assisted_sessions", DataType::Int64, false),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(id.finish()),
            Arc::new(ch.finish()),
            Arc::new(Date32Array::from(day)),
            Arc::new(Int64Array::from(s)),
            Arc::new(Int64Array::from(p)),
            Arc::new(Int64Array::from(r)),
            Arc::new(Int64Array::from(a)),
        ];
        write_batch(&cd.join("part-0.parquet"), RecordBatch::try_new(schema, cols).unwrap());
    };

    let user_jobs = || {
        aggs.par_iter().enumerate().for_each(|(i, a)| {
            if a.rows.is_empty() && i > 0 {
                return;
            }
            let n = a.rows.len();
            let bytes: usize = a.rows.iter().map(|r| (r.name & 0xFFFF) as usize).sum();
            let mut ids = StringBuilder::with_capacity(n, bytes);
            let mut top = StringBuilder::with_capacity(n, n * 4);
            let mut day = Vec::with_capacity(n);
            let (mut s, mut e, mut r) = (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
            for row in &a.rows {
                ids.append_value(utf8(name_bytes(row.name)));
                day.push(row.day);
                s.push(row.sessions as i64);
                e.push(row.events as i64);
                r.push(row.revenue);
                if row.top == 255 {
                    top.append_null();
                } else {
                    top.append_value(utf8(&cats[row.top as usize]));
                }
            }
            let cols: Vec<ArrayRef> = vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(Date32Array::from(day)),
                Arc::new(Int64Array::from(s)),
                Arc::new(Int64Array::from(e)),
                Arc::new(Int64Array::from(r)),
                Arc::new(top.finish() as StringArray),
            ];
            write_batch(&ud.join(format!("part-{i}.parquet")), RecordBatch::try_new(schema.clone(), cols).unwrap());
        });
    };
    rayon::join(campaign_job, user_jobs);
}
