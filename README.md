# Shopping Sessions — exact, parallel Rust pipeline

Builders Buildathon 2026, Databricks track. Reads the dirty CSV shards in `/data`
and writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet`
exactly as `rules.md` defines them.

The image is a single statically linked binary on `scratch`, so container start-up
adds almost nothing to the measured time.

## How it works

1. **Input.** Every CSV is memory-mapped. Work is split into ~4 MiB line-aligned
   units (a line belongs to the unit holding its first byte), and each unit is
   processed in 192 KiB sub-blocks so the second pass hits L2.
2. **Tokenizing (simdjson-style).** Stage 1 uses AVX2 to build newline / comma / quote
   bitmaps 64 bytes at a time. Stage 2 walks lines from the newline bitmap and slices
   out 128-bit comma/quote windows. The CSV dialect is checked with bit tricks and no
   per-byte branches. A quote must sit at a field start or end, quotes balance with a
   prefix XOR, and no comma may fall inside quotes. Anything else is a malformed line.
3. **Timestamps.** F1/F2 use table-driven calendar maths that rejects impossible dates,
   hour 24, second 60 and bad offsets. F3 uses an 8-digit SWAR parser.
4. **Links.** These are resolved in parallel with event parsing: a union-find over hashed
   IDs, with each component named by its byte-wise smallest ID.
5. **Event dedup.** Copies are partitioned by `event_id` hash (512 buckets). Each bucket
   keeps the earliest-ingested copy with a fingerprint hash table, then validates it,
   so a later valid copy never rescues an invalid earliest one. Users are mapped to
   persons with prefetched lookups, and survivors are scattered by person hash
   (1,024 buckets).
6. **Clicks.** These are parsed, validated, mapped to persons and bucketed the same way.
   Duplicate copies of a click are identical (`rules.md`), so they cannot change the
   credit (latest `ts`, then smallest `click_id`) or the distinct-campaign assists.
   Explicit click dedup is therefore unnecessary.
7. **Sessions and attribution.** Inside each person bucket, events and clicks are grouped
   per person by counting sort, then sorted by time. Sessions split only on gaps over
   30 minutes. A sliding 7-day click window gives the credited click and the set of
   distinct assisting campaigns in O(clicks + sessions + assists).
8. **Output.** Uncompressed Parquet with `DATE` days, one `user_daily` file per worker,
   written in parallel.

Large buffers come from a small allocator that maps 2 MiB-aligned regions advised for
transparent huge pages. That cuts page faults on the ~1 GiB of fresh records by
roughly 4×.

## Correctness checks

- 36/36 official edge fixtures.
- The official 1M dev set matches `dev-answer` exactly (sorted-row SHA-256 of both tables).
- Generated datasets for 10 other seeds were diffed row-for-row against an independent
  Polars implementation. They included leap-day (Feb 29, 2028) and year-crossing
  windows, and a 10M-event run (1.14M user-day rows).
- Contract-respecting fuzzing (`tools/fuzz.py`) adds CR bytes, stray and partial
  quotes, whitespace, case changes, odd timestamp forms, empty lines, a missing final
  newline, 0-byte files and non-CSV files. Rust, Polars and a literal pure-Python
  reference (`tools/ref.py`) agree on every round.

`tools/check.py` compares an output folder with expected CSVs, with another output
folder, or with the dev-answer hashes.

## Build and run

```bash
docker build -t shopping .
docker run --rm --network none --read-only --user 1000:1000 \
  --cpus 8 --memory 24g --tmpfs /tmp:rw,size=64m \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e NCPU=8 \
  -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" shopping
python3 tools/check.py out --hash        # dev set: prints dev_match=True for both tables
```

Locally: `cargo build --release && DATA_DIR=dev OUTPUT_DIR=out NCPU=8 target/release/ssp`
(`SS_TIMING=1` prints stage timings).

## Dependencies and disclosure

Open-source crates: `rayon`, `memmap2`, `xxhash-rust`, `arrow-array`, `arrow-schema`,
`parquet` (Apache Arrow), `libc`. The verification scripts use `pyarrow`. This
solution was written with help from an AI assistant (Claude), as the Buildathon
rules allow.

MIT licensed.
