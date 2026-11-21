# Exact Shopping Sessions pipeline

Rust solution for the [Builders Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions). The target is a verified official runtime below 0.4 seconds. **That target has not been achieved or verified.** Local and GitHub Actions measurements are not official scores.

The original submission is preserved at `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. This branch adapts the repository's subsequent v11.3 architecture (`0edb1e785c36ae85cdf53faddcfc319b7d424d48`), adds exact identifier representations and edge-case repairs, and measures event/copy compaction. The inherited architecture and new experiments are distinguished in `experiments/SUB400_PROGRESS.md`.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping .
mkdir -p out/user_daily out/campaign_daily
chmod -R a+rwX out
docker run --rm --network none --read-only --user 1000:1000 \
  --cpus 8 --memory 24g --memory-swap 24g --tmpfs /tmp:rw,size=64m \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" shopping
```

The root Dockerfile pins the Rust builder image and produces a static Linux/AMD64 executable in a scratch image. Runtime needs no network. Input/output default to `/data` and `/output`; `DATA_DIR`, `OUTPUT_DIR` and `NCPU` override them. Do not commit generated data. An optional BuildKit `build_ca` secret supplies a local proxy's trusted CA bundle during compilation; ordinary public builds need no secret.

For direct runs: `cargo build --release --locked`, then `DATA_DIR=data OUTPUT_DIR=out NCPU=8 target/release/ssp`. AVX2/BMI2 and PCLMULQDQ retain the upstream CPU requirements; newer instruction sets require runtime dispatch.

## Data flow and exactness

* Memory-map input; partition scans into line-owned chunks with a SIMD dialect tokenizer. Only LF separates lines. Malformed quotes and field counts are rejected; CR is preserved.
* Resolve literal identity edges with parallel union-find; choose each component's byte-wise smallest ID. A bijective integer permutation of reversible ASCII keys or exact interned bytes replaces probabilistic identifier equality. No identifier bits are discarded.
* Route valid event candidates directly to person streams. Keep every parseable event copy, including invalid candidates, in an independent deduplication stream. Earliest ingestion wins before validation can affect the result. Rare dates outside unsigned epoch seconds retain full signed comparisons.
* Use 16-byte surviving-event candidates and 16-byte copy records. Event IDs have a lossless base62 encoding when possible; every other ID is interned exactly. Wide references, stream locations and timestamps use explicit side tables instead of truncation.
* Deduplication marks losing candidates dead. Group surviving events by person and half-hour bins. Track each bin's minimum and maximum to form sessions under the strictly-greater-than-30-minute rule without sorting every event timestamp. Click ordering, inclusive seven-day attribution and distinct assistance are preserved.
* Decode user names only when writing output. Write standards-compliant uncompressed Parquet directly, with DATE logical types, integer counts, nullable UTF-8 categories and 64-bit revenues. Row order and file count may differ.

The default release build contains no per-record diagnostic counters. Build with `--features diagnostics` and set `SS_PROFILE=1` for independent wall/process/thread CPU spans, partition sizes and allocation diagnostics. `SS_TIMING=1` prints cumulative barriers. CPU times sum execution across threads and must not be treated as wall time. `tools/bench.py` interleaves variants and records every start-to-exit sample, CPU, RSS, faults and IO counts.

## Validation

`tools/validate.py BINARY --threads 8 --fuzz-rounds 5` runs all 36 official fixtures, the downloaded official development answer, and a literal Python reference on the small generated input plus four dirty-input mutations. `tests/adversarial.py BINARY` checks long UTF-8 identifiers, dates before 1970 and transformation boundaries. `tools/check.py` compares both complete tables while ignoring permitted row/file ordering. Release unit tests exercise tokenizer equivalence, identity domains and inverses, compact copy bit layouts and overflow side tables.

See the experiment report and raw results for the exact tested artifacts and sample distributions. The fresh hosted check passed at20M and60M for exact source commit `fef07368caf2e161fbf91dbf67e8176574fcfaa9`:20M935.15ms versus1550.86ms baseline,60M2857.89ms versus4816.35ms, on4 CPUs. Full evidence is in `FIRST_SUBMISSION_REPORT.md` and `submission/READY.json`. A successful GitHub check or repository push is not an official competition submission.

## Dependencies and license

MIT licensed. Rust dependencies: `libc` (MIT/Apache-2.0), `memmap2` (MIT/Apache-2.0), `rayon` (MIT/Apache-2.0), `xxhash-rust` (BSL-1.0). Validation uses Python, PyArrow (Apache-2.0) and Polars (MIT). The official generator and fixtures are downloaded for testing and are not shipped as competition data. Development used Claude and OpenAI Codex, as permitted by the competition.
