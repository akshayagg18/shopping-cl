# GPT-6 verified performance experiments (November 21, 2026)

Base: Claude's `shopping-cl` Rust pipeline, exact baseline `110c725db24a1768ee10f626a83ed96b5912ec11` for this merge's A/B comparison. Original user-submitted commit: `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.

## Research findings

- Claude's architecture already uses mmap, AVX2 scanner, a parallel hash-bucket dedup pipeline, per-person sorting, and direct Parquet output.
- Earlier no-sort and interval-union experiments did not beat it on the official 20M-format dataset, despite passing the dev hash checks.
- A public MIT-licensed competitor (JuanQuenga/buildathon-shopping-sessions) measured memory-release and page-fault overhead; this prompted a separate parallel-release experiment. We used our own source modifications, not their code.
- Four workers beat eight on GitHub's 4-core benchmark in prior controlled experiments; the scored machine reportedly has 4 physical cores with SMT and 8 logical threads. The hard cap to 4 is experimental for the official scorer.

## Controlled A/B results on 20M events, generated seed 20 and 5M clicks

| Experiment (each vs Claude V3) | Baseline median | Candidate median | Result |
| --- | ---: | ---: | --- |
| Chunk size 4M to 16M | 1.613s | 1.654s | rejected (2.6% slower) |
| Compact event slabs | 1.944s | 1.969s | rejected (1.3% slower) |
| Direct catalog lookups with generic fallback | 1.663s | 1.642s | 1.3% faster, marginal |
| Parallel release of event/click buffers | 1.668s | 1.587s | 4.9% faster |

Individual GitHub-hosted runner timings fluctuate; compare paired samples, not cross-run absolutes. The full 20M process was timed, including Parquet output. These are NOT official contest scores.

## Integrated candidate

- Exact catalog ID direct-index lookups (noncanonical or unknown strings use existing hash-map fallback).
- Parallel release of large buffers after their last use.
- Cap Rayon workers at four, based on prior isolated GitHub A/B; still allow a lower explicit NCPU.
- All other original rule semantics remain unchanged.
- The benchmark workflow compares this combined candidate against the parallel-release-only baseline; checks both official published dev-answer SHA hashes.

Never submit if official dev fixtures fail or if the candidate is slower than the current scored commit. Preserve the original submission.
