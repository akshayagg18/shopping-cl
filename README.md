# Shopping Sessions — Codex optimization branch

Validated scorer commit: [`1ffcf309d044cf44d38ed49c071308b73b46118c`](https://github.com/akshayagg18/shopping-cl/commit/1ffcf309d044cf44d38ed49c071308b73b46118c).

Builders Buildathon 2026, Databricks track. Reads every CSV shard under `/data`
and writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet`.
Days use Parquet DATE; comparison is exact and independent of row/file order.

The original Claude commit remains intact:
`d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.
This branch adds measured optimizations and fixes inherited collision/reference
risks. See [FIRST_SUBMISSION_REPORT.md](FIRST_SUBMISSION_REPORT.md),
[research.md](research.md), and the [experiment ledger](experiments/EXPERIMENTS.md).
Local measurements are not official scores.

## Architecture

- AVX2 CSV structural bitmaps, strict whole-field quotes, LF-only physical lines,
  ASCII-only numeric parsing, exact timestamp windows and calendar validation.
- Transitive union-find names each component by its byte-wise smallest full ID.
- Short ASCII IDs use reversible integer encodings. Other UTF-8 IDs use hashes
  with full byte comparisons; unlinked names receive exact interned keys.
- Events use compact records while retaining invalid copies for earliest-ingest
  deduplication. An invalid earliest copy never gets rescued by a later copy.
- Names are recovered from linked-name refs or decoded from exact keys when
  writing, rather than copied through every intermediate event representation.
- Person partitioning, per-person ordering,30-minute sessions, inclusive 7-day
  click windows, byte-wise click tie-breaks, distinct campaign assists and exact
  purchase/category reduction follow the official rules.
- A bounded number of persistent workers acquire partitions dynamically and
  write uncompressed Parquet. Buffers use initialized prefixes of MaybeUninit
  storage; overflow references preserve long UTF-8 IDs without truncation.

Input/window/category limits follow the official generator (14-day windows,
20 categories,2,000 campaigns, up to 60M events). No seed, time window, row value,
expected result, generated input, or runtime network dependency is embedded.

## Build and run

```bash
docker build --platform linux/amd64 -t shopping-codex .
mkdir -p out scratch
chmod 777 out scratch
docker run --rm --network none --read-only --user 1000:1000 \
  --tmpfs /tmp:rw,size=64m -e NCPU=8 -e MEM_GB=24 \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" -v "$PWD/scratch:/scratch" shopping-codex
```

BuildKit is required (the default in modern Docker). Behind a custom build proxy,
pass `--secret id=build_ca,src=/path/to/combined-ca-bundle.pem`; the optional CA
secret is mounted only for Cargo downloads. TLS verification stays enabled, and
no proxy certificate enters the image. Public builds need no secret.

The root Dockerfile pins the builder image digest, locks crate versions, targets
x86-64-v3 and produces a static linux/amd64 executable on scratch. Docker build
has network access; execution needs none. The MIT notice is included in the image.
The official environment supplies 8 pinned hyperthreads and 24 GB RAM. This local
cloud VM has a 2-CPU quota and 8 GiB; do not compare its absolute times to the 1.73 s
user-reported practice score or infer leaderboard rank from a push.

## Reproduce validation and measurements

With Rust/cargo, Python, pyarrow and polars installed:

```bash
python tools/bootstrap.py       # downloads official assets; generates ignored inputs
cargo build --release --locked
cargo test --release --locked
python tools/validate.py target/release/ssp --threads 8
python tests/adversarial.py target/release/ssp
python tools/check.py out --expected experiments/assets/answer/dev-answer
```

Validation uses 36 official fixtures, the public seed-1 1M dev answer, the supplied
literal Python reference, supplied fuzz mutations, forced collisions, UTF-8 IDs
beyond 65,535 bytes, ingestion dates before 1970 and parser block boundaries.
The historical README's separate Polars reference was not present; the official
Polars starter is an empty skeleton. It is not claimed as reproduced.

Generate larger local workloads with `experiments/bin/generate`; do not version
them. `tools/bench.py` interleaves control/candidate runs, supports explicit
warmups/CPU affinity and records wall time, CPU time, RSS samples/high-water
mark, faults, context switches and input/output blocks. `tools/summarize.py`
reports medians, means, sample deviations, min/max and barrier deltas.
`tools/docker_bench.py` reports container-state start-to-exit separately from
Docker CLI overhead. Always use fresh output directories between datasets.

`SS_TIMING=1` prints cumulative barriers. `SS_PROFILE=1` additionally prints
noncumulative wall/thread/process CPU spans, worker/record/byte counts and
allocation/resource diagnostics. Parallel spans overlap; do not sum them into
elapsed time. `SS_SEQ=1` separates concurrent stages for diagnosis.
`SS_LAZY=1` disables eager mmap population for controlled experiments only.

## License and dependencies

MIT; original notice retained. Open-source dependencies: rayon, memmap 2,
xxhash-rust, arrow-array, arrow-schema, parquet (Apache Arrow) and libc; exact
transitive versions/checksums are in Cargo.lock. Validation used pyarrow 26.0.1
and polars 2.1.0. Claude authored the pinned baseline; Codex performed this
branch's analysis, implementation, validation and experiments.
