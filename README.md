# Shopping Sessions — experimental dense Rust pipeline

**Do not submit commit `6308907` for the final evaluation.** It passed fresh
Docker and exact 60M validation, but was nearly twice as slow as the prior
commit at 60M. The recommended validated scorer source remains
`fef07368caf2e161fbf91dbf67e8176574fcfaa9`. See `submission/READY.json`.

Processes `/data` into exact `/output/user_daily` and `/output/campaign_daily`
Parquet tables. Build the root Dockerfile for Linux/AMD64; the static scratch
runtime needs no network and runs as uid 1000.

The new default path learns a reversible ID layout from input labels, resolves
only literal identity links, deduplicates 24-byte event records before emitting
winners, then groups literal person runs and clicks. Unrecognized labels and
unsupported layouts use the exact general path. Full signed ingestion and long
UTF-8 labels retain lossless fallbacks. CSV dialect, UTC dates, session boundaries,
click ties, distinct assists and category ties retain the specified semantics.

Local 20M median 2098.98 ms versus 2415.39 ms for fef0736, nine interleaved runs each
on a 2-CPU VM: 13.1% less elapsed time. Both output tables match the frozen baseline.
These are local measurements, not an official score. Fresh Docker and 60M correctness checks passed; the 60M performance gate failed. The previous fully validated scorer source is
`fef07368caf2e161fbf91dbf67e8176574fcfaa9`; see `submission/READY.json`.

Reproduce controls with `SS_DENSE=0`; `SS_TIMING=1` reports stage envelopes.
Default release compiles out diagnostics; build `--features diagnostics` and set
`SS_PROFILE=1` for per-task wall/CPU, records,bytes and allocation measurements.
See `experiments/DENSE_RUST_EXPERIMENTS.md` for hypotheses, distributions, rejections,
correctness coverage and provenance. Source uses no other entrant's engine.

Dependencies are open source: libc (MIT/Apache-2.0), memmap2 (MIT/Apache-2.0),
Rayon (MIT/Apache-2.0), xxhash-rust (BSL-1.0). This deliverable is MIT licensed;
see LICENSE. Generated data and answers are excluded from the repository.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping .
mkdir -p out/user_daily out/campaign_daily
chmod -R a+rwX out
docker run --rm --network none --read-only --user 1000:1000 \
  --cpus 8 --memory 24g --memory-swap 24g --tmpfs /tmp:rw,size=64m \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" shopping
```

Input/output default to `/data` and `/output`; `DATA_DIR`, `OUTPUT_DIR`, `NCPU`
override them. For direct runs, use `cargo build --release --locked` then
`DATA_DIR=data OUTPUT_DIR=out NCPU=8 target/release/ssp`. Target CPUs need
x86-64-v3 and PCLMULQDQ, as in the inherited implementation.

## Validation and provenance

`tools/validate.py BINARY --threads 8 --fuzz-rounds 5` runs 36 official fixtures,
the official development answer, and a literal reference on small data plus four
fuzz mutations. `tests/adversarial.py BINARY` checks long UTF-8 names, pre-1970
ingestion and boundaries. `tools/check.py` compares complete tables while
ignoring permitted row/file order. `cargo test --release --locked` runs 26 units.
Downloaded fixtures, generator, data and answers are excluded from the repository.

Frozen baseline: `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. The inherited v11.3
starting point was `0edb1e785c36ae85cdf53faddcfc319b7d424d48`. Exact repairs and
previous compact pipeline are documented in `FIRST_SUBMISSION_REPORT.md`.
This dense Rust design is independently implemented. Only literal input links
establish identity; label decoding selects storage indices and always has exact
fallbacks. No seed,window,product mapping or expected result is hard-coded.

The goal is an official runtime below 0.4 seconds. No such result is verified.
CI results and repository pushes do not constitute official submissions.
