# Sub-0.4-second investigation, 2026-11-21

No official sub-0.4-second receipt exists. All figures here are measured local
start-to-exit runtimes on a Xeon 8573C VM with a **2-CPU quota**, 8 GiB RAM,
affinity 0,2, one warmup and interleaved variant order. They are not official
hardware, and separate batches' ratios must not be multiplied.

## Architectural assessment

The initial pinned source (`d2dc441`) was reviewed and frozen before changes.
Its repeated 40-byte event gathers, serial per-person comparison sorts and
Arrow serialization were credible costs, but many compact/fused variants
regressed. Earlier independently validated source `1ffcf309` achieved an
11.6% local 20M improvement and exact outputs. A standard public GitHub runner
also validated that earlier source at 60M: 5553.3 to 5233.9 ms (three pairs,
4 CPUs, AMD EPYC 9V45, 16 GiB, no swap). Those are **earlier-source** results,
not results for this branch.

During this investigation, upstream main advanced to v11.3 (`0edb1e7`). Its
new architecture routes candidates once, marks duplicate losers dead, builds
identity links in parallel, forms sessions from half-hour-bin extrema and
writes Parquet directly. These are inherited improvements, not inventions
claimed by this branch. They reduced a paired local median from 3407.6 to
2648.8 ms (nine pairs). However, unmodified upstream failed the long-UTF-8 and
pre-1970-ingestion adversarial input. It could not be selected as a correct
submission unchanged.

This branch repairs identifier exactness, wide references, full signed
ingestion comparison and uninitialized vector lengths. A second nine-pair
batch measured repaired v11 at 2550.2 ms versus upstream's 2482.7 ms. Exactness
cost about 2.7% there. No collision probability is substituted for ID equality.

## Controlled experiments after repair

| Experiment | Control median ms | Candidate median ms | n each | Correctness | Decision |
|---|---:|---:|---:|---|---|
| 48-bit exact IDs, 16-byte copy records |2527.0|2537.1|9|Fixtures/dev/fuzz/adversarial passed|Memory gain only; ~200 MiB saved|
| Remove user reference, 16-byte event candidates |2579.8|2474.8|9|Fixtures/dev/fuzz/adversarial/20M passed|Keep; 4.1% median gain|
| Both record widths versus repaired v11 |2503.0|2441.2|9|Both complete 20M tables passed|Keep compact candidate, confirm independently; mean effectively flat in this noisy batch|
| 16-byte events, original 24-byte copies |2503.0|2476.3|9|Fixtures/dev/fuzz/adversarial passed|Slightly slower median than compact copies|
| Paired AVX2 event/ingestion time decoding |2624.4|2606.2|9|Mutation property test and all integration checks passed|Reject; mean worse, no convincing gain|
| Conservative linked-ID membership filter |2548.1|2742.5|9|All integration checks passed|Reject; 7.6% slower|

Every sample, CPU/RSS/fault counts, mean, SD, minimum and maximum is retained in
`results/sub400-*.json`. Rejected prototypes are excluded from production.
A sampled direct user cache hit only 5.6% at 256 entries and about 14% at 4096
entries, so it was not implemented. The generator shuffles files; locality must
be measured, not inferred from per-user generation code.

## Profiling and next priorities

Callgrind 3.24.0 on the 1M development input, NCPU=1, counted 1,390,073,518
instructions for repaired v11. Event scan/validation self instructions were
47.44%, the tokenizer front end 11.49%, session/grouping 15.59%, click scan
8.56%, and dedup 1.67%. This is instruction/cache **simulation**, not physical
hardware counters. `perf_event_open` is denied in this VM. Parser and identity
work remain the priority; dedup micro-optimizations alone cannot meet the goal.

The diagnostics feature adds stage/task wall time, process/thread CPU time,
record and byte counts, worker identifiers and allocation counts. It compiles
out of the default release. CPU envelopes overlap for nested concurrent tasks;
they must not be added to wall time. RSS is sampled externally by the benchmark.

Earlier real-byte cached IO probes on this VM measured 6.637 GB/s scanning and
1.679 GB/s writing. For 1,883,811,848 input bytes and approximately 79.2 MB
output, the empirical IO-only estimate is about 283.8 ms reading plus 47.2 ms
writing (331.0 ms serialized). This is not a proven physical floor and cannot
be extrapolated to the official machine. Compute, faults and scheduling remain.

The next new front-end hypothesis is runtime-dispatched AVX-512/VBMI2 separator
compaction, retaining AVX2 on CPUs without those features. No unmeasured speed
claim is made for it. Further progress needs a full paired static-image result,
60M correctness/scaling evidence and an actual official scorer receipt.
