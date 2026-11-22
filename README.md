# Shopping Sessions — exact, parallel Rust pipeline

Builders Buildathon 2026, Databricks track. Reads the dirty CSV shards in `/data`
and writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet`
exactly as `rules.md` defines them.

The image is a single statically linked binary on `scratch`, so container start-up
adds almost nothing to the measured time.

## How it works

1. **Input.** Work is split into ~4 MiB line-aligned units (a line belongs to the unit
   holding its first byte). Each unit is read with `pread` in 192 KiB windows into a
   per-thread buffer that stays in cache; files are opened per unit (a large fd table
   would grow, and each growth waits for an RCU grace period).
2. **Links first, in parallel.** Link endpoints are hashed and scattered into 256
   partitions, numbered per partition, joined with a lock-free union-find (a root only
   links to a smaller root), and every component is named by its byte-wise smallest ID
   with a parallel compare-and-swap minimum. The person map is a sorted open-addressing
   table built in 256 independent ranges.
3. **Tokenizing.** One fused AVX2 pass per 64 bytes builds the newline bitmap, a bitmap
   of quoting violations (prefix-XOR quote parity: no separator inside quotes, opening
   quotes after a separator, closing quotes before one) and a flat list of separator
   positions. A line is then: next newline from the bitmap, one compare that its N-th
   separator is that newline, one test of the violation bitmap. Fields are unquoted on
   access.
4. **Values.** F1/F2/F3 timestamps take inlined fast paths with a small per-thread cache
   of validated `YYYY-MM-DD` dates; anything unusual falls back to the strict general
   parser (impossible dates, hour 24, second 60, bad offsets are rejected). Short IDs
   are hashed with two AES rounds (a bijection of the zero-padded 16-byte ID).
5. **One scatter per record.** With persons known up front, each valid event goes
   straight to its person partition (24-byte record), and every event copy, valid or
   not, leaves a 24-byte copy record in its `event_id` partition. Records leave through
   per-partition write-combining staging areas with non-temporal stores. Person lookups
   are prefetched 16 lines ahead. An unlinked user's ID is copied into a per-thread name
   buffer while the line is in cache, so output never reads the event files again.
6. **Dedup.** Per `event_id` partition, a fingerprint table keeps the earliest-ingested
   copy (dedup before validation: an invalid winner kills its valid copies) and marks
   every losing copy's event record dead in a bitmap. Click copies are identical
   (`rules.md`), so they need no dedup.
7. **Sessions without sorting.** Inside each person partition (largest first), events
   are grouped per person (8-byte events and 16-byte clicks from then on, so a partition
   stays in L2) and ordered by half-hour bin with two counting passes. Two
   events in one bin are never more than 30 minutes apart, so sessions are cut between
   bins only (by each bin's min and max). A sliding 7-day click window gives the
   credited click (latest `ts`, then smallest `click_id`) and the distinct assisting
   campaigns.
8. **Output.** A small hand-written Parquet writer (PLAIN, uncompressed, one page per
   column, `DATE` days) writes one `user_daily` file per worker in parallel.
9. **Exit.** The process exits without freeing anything. Event files are never mapped
   into memory pages, so the kernel's single-threaded teardown has little to do.

Partition counts scale with the input size, so the 60M final keeps the same per-partition
working sets as the 20M practice set. Large buffers come from 2 MiB-aligned mappings
advised for transparent huge pages.

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

Open-source crates: `rayon`, `memmap2`, `xxhash-rust`, `libc` (the Parquet writer is
our own). The verification scripts use `pyarrow`. This solution was written with help
from an AI assistant (Claude), as the Buildathon rules allow. Some techniques (half-hour
bins instead of sorts, write-combining scatter, links before parsing) were inspired by
notes in public Shopping Sessions repositories; no code was copied.

MIT licensed.
