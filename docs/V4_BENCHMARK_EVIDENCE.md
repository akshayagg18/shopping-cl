# Shopping Sessions V4 — verified benchmark evidence

This experimental branch is based on the author's Claude-owned `shopping-cl` V3 commit `bc272b815059c02dd95edfeb0217b34c65667d34`.

## Changes

- Lazy memory mapping: remove eager prefaulting during file open, letting independent workers touch their input regions.
- Parallel release of large temporary event/click partitions after use.
- Inline insertion sorts for event and click lists of 2–12 records; exact timestamp / bytewise click-ID tie rules preserved.
- Preserve the original Rust AVX2 CSV engine, SWAR timestamp validation, event-ID dedup, identity unions, per-person aggregation, and direct Parquet output.

## Measured isolated experiments

All are paired five-versus-five whole-process runs on 20M synthetic official-generator events and 5M clicks on GitHub Actions; timings include Parquet output. Different rows use different, potentially noisy runner sessions.

| Experiment | Control median | Modified median | Outcome |
|---|---:|---:|---|
| Lazy mmap | 1.4932s | 1.3345s | 10.6% faster |
| Parallel release | 1.6684s | 1.5867s | 4.9% faster |
| Combined lazy+release | 1.4012s | 1.3563s | 3.2% faster |
| Tiny insertion sorting alone | 2.2592s | 2.2116s | 2.1% faster |
| Larger 16MiB parsing chunks | 1.6127s | 1.6544s | rejected |
| Smaller event slabs | 1.9439s | 1.9691s | rejected |
| Four workers instead of eight | 1.5985s | 1.6562s | rejected |
| 96KiB parser blocks | 1.4424s | 1.4921s | rejected |
| Packed event IDs | 2.0049s | 2.3490s | rejected |
| Whale bitset assists | 2.2667s | 2.3144s | rejected |

The combined V4 branch is independently A/B-tested on its own GitHub Actions run; do not infer total speed from the isolated results.

## Correctness and caveats

Each isolated experiment that succeeded in CI matched both published dev-answer SHA-256 outputs. The final combined candidate additionally compares its entire 20M output to baseline in CI; inspect the resulting workflow run.

The 1.73s figure is the user's reported **official** score for the earlier Rust commit, not a locally reproduced score for this branch. Local GitHub Actions timings are not official leaderboard scores, and none of these results establishes a sub-0.5-second official run.

Related public implementation techniques were studied from `krishchopda/shopping-sessions`, `JuanQuenga/buildathon-shopping-sessions` and `matthewf725/shopping-sessions`. Their code was not copied into this branch.
