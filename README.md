# Shopping Sessions — exact, parallel Rust pipeline

Builders Buildathon 2026, Databricks track. Reads the dirty CSV shards in `/data`
and writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet`
exactly as `rules.md` defines them.

The image is a single statically linked binary on `scratch`, so container start-up
adds almost nothing to the measured time.

## How it works

1. **Input.** Every CSV is memory-mapped. Work is split into ~4 MiB line-aligned units
   (a line belongs to the unit holding its first byte); workers take them in list order
   from a shared counter. If the input is not in the page cache (a few pages per file
   checked with `mincore`), a helper thread reads it ahead with `MADV_WILLNEED` in that
   same order while links are being resolved.
2. **Links first, in one concurrent table.** Every link endpoint is inserted into one
   open-addressing table with compare-and-swap; a slot's index is the ID's node number.
   A lock-free union-find (a root only links to a smaller root) joins the nodes, each
   component is named by its byte-wise smallest ID with a compare-and-swap minimum, and
   the same table, its values rewritten to `LINKED | name ref`, is the person map. No
   sorting, scattering or serial pass.
3. **Tokenizing.** One fused AVX2 pass per 64 bytes builds the newline bitmap, a bitmap
   of quoting violations (prefix-XOR quote parity: no separator inside quotes, opening
   quotes after a separator, closing quotes before one) and a flat list of separator
   positions. A line is then: next newline from the bitmap, one compare that its N-th
   separator is that newline, one test of the violation bitmap. Fields are unquoted on
   access. (A per-line variant, three 32-byte loads per line, is faster on newer cores
   but measured ~110 ms slower on the scorer's Cascade Lake: it stays off.)
4. **Values.** F1/F2/F3 timestamps take inlined fast paths with a small per-thread cache
   of validated `YYYY-MM-DD` dates; anything unusual falls back to the strict general
   parser (impossible dates, hour 24, second 60, bad offsets are rejected). Short IDs
   are hashed with two AES rounds (a bijection of the zero-padded 16-byte ID).
5. **One scatter per record.** With persons known up front, each valid event goes
   straight to its person partition (16-byte record), and every event copy, valid or
   not, leaves a 16-byte copy record in its `event_id` partition (the hash bits that
   pick the partition are reused for check bits). Records leave through
   per-partition write-combining staging areas with non-temporal stores. Person lookups
   are prefetched 16 lines ahead. A linked person's key holds its name; an unlinked
   ID of up to 10 characters from `[0-9a-zA-Z]` is packed losslessly into its own key
   (6 bits a character), so it needs no name at all. Only other unlinked IDs (long or
   unusual ones, about 5% of events) leave a (key, name) pair in a small side table.
6. **Dedup.** Per `event_id` partition, a fingerprint table keeps the earliest-ingested
   copy (dedup before validation: an invalid winner kills its valid copies) and marks
   every losing copy's event record dead in a bitmap. Click copies are identical
   (`rules.md`), so they need no dedup.
7. **Sessions without sorting.** Inside each person partition (largest first), events
   are grouped per person with one counting pass (8-byte events and 16-byte clicks from
   then on, so a partition stays in L2); each person's events are binned by half hour
   into a bitmap of occupied bins. Two events in one bin are never more than 30 minutes
   apart, so sessions are cut between bins only (by each bin's min and max). A sliding 7-day click window gives the
   credited click (latest `ts`, then smallest `click_id`) and the distinct assisting
   campaigns.
8. **Output.** A small hand-written Parquet writer (PLAIN, uncompressed, one page per
   column, `DATE` days) writes one `user_daily` file per worker in parallel.
   Rows are split into equal shares, one file per thread.
9. **Exit.** The process exits without freeing anything. Event files are never mapped
   into memory pages, so the kernel's single-threaded teardown has little to do.

Partition counts scale with the input size up to 1024 each: on the scorer at 60M,
4096 person and event-id partitions made parsing about 18% slower (the staging areas
leave the 1 MiB L2) for little gain in dedup and sessions (2.88 s against 2.62 s end to
end). Large buffers come from 2 MiB-aligned mappings advised for transparent huge pages.

## Correctness checks

- 60M (scorer, 6.7 GB input made of 60 tagged copies of the dev set): an order-independent
  digest of both tables equals 60 times the dev digest, for every partition setting
  tried; peak memory 10.8 GB of the 24 GB limit.

- 36/36 official edge fixtures.
- Scorer v5 data shape (1 in 8 IDs with a 32-character hex suffix): generated sets diffed
  row for row against an independent Polars implementation.
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
