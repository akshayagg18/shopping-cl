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
3. **Tokenizing.** Per window of 192 KiB, one AVX2 pass builds the newline bitmap, so
   every line's start and length are known up front. A line of up to 191 bytes is then
   split from three to six 32-byte loads: comma and quote masks, prefix-XOR quote
   parity, and the quoting rules (no separator inside quotes, opening quotes after a
   separator, closing quotes before one) as a few mask operations; the N-th separator
   must be the newline. Longer lines (none in generated data) take a scalar path. Fields are
   unquoted on access. (With three loads only, the fifth of scorer-v5 lines that carry a
   32-character ID did not fit, which is why this tokenizer first measured slower than
   the stage-1 bitmap one, still available as `SS_TOK=1`.)
4. **Values.** F1/F2/F3 timestamps take inlined fast paths with a small per-thread cache
   of validated `YYYY-MM-DD` dates; anything unusual falls back to the strict general
   parser (impossible dates, hour 24, second 60, bad offsets are rejected). Short IDs
   are hashed with two AES rounds (a bijection of the zero-padded 16-byte ID).
5. **One scatter per record.** With persons known up front, each valid event goes
   straight to its person partition (24-byte record), and every event copy, valid or
   not, leaves a 16-byte copy record in its `event_id` partition (the hash bits that
   pick the partition are reused for check bits). Records leave through
   per-partition write-combining staging areas with non-temporal stores. Person lookups
   are prefetched 16 lines ahead. An unlinked user's ID is copied into a per-thread name
   buffer while the line is in cache, so output never reads the event files again.
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
leave the 1 MiB L2) for little gain in dedup and sessions (2.88 s against 2.53 s end to
end, same machine). Large buffers come from 2 MiB-aligned mappings advised for
transparent huge pages.

At 60M (scorer: 60 tagged copies of the dev set, 6.7 GB) an order-independent digest of
both tables equals 60 times the dev digest; peak memory 10.6 GB of the 24 GB limit.

## Build: profile-guided

The image is built with profile-guided optimization: `pgo/ssp.profdata` is the profile
of an instrumented build of this source run on our own generated data (2M events in
the scorer-v5 ID format, 8 and 2 threads), merged with `llvm-profdata`. The compiler
is pinned to rust 1.97.0, the version that made the profile. A profile only changes
code layout and inlining, never results. On the scorer (same container, alternating
runs) the parse phase took 1496 ms against 1576 ms without the profile at 60M, and the
run 785 ms against 840 ms for v16b at 20M. `tools/pgo.sh` regenerates the profile.

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
