# Initial assessment before optimization

See research.md for complete source review and inherited correctness risks; experiments/PROTOCOL.md for limitations. Frozen pinned baseline passed official 36/36 fixtures, official seed-1 1M CSV comparison, 20K seed-314 literal reference and 4 supplied fuzz mutations (5 reference comparisons total). Tests use fresh output directories; otherwise variable Rayon output-file counts can leave stale rows during local reruns.

5M events/1.25M clicks, official generator seed 42: 479,967,663 CSV bytes. VM quota 2 CPU/8 GiB vs official 8 hyperthreads/24 GB. Five diagnostic parallel runs:

| Stage | Median wall ms | Normalized share of median barrier deltas | Suspected bottleneck | Potential |
|---|---:|---:|---|---|
| mmap/open | 9.5 | 1% | page tables; no physical reads in warm runs | low |
| lookups, links + event parse | 414.4 | 43% | timestamp/CSV validation, record stores | high |
| dedup + click parse | 254.9 | 26% | hashing, gather/scatter, random identity lookup | medium/high |
| sessionization + aggregate | 230.1 | 24% | group/sort, repeated state allocations | medium |
| Arrow/Parquet write | 62.5 | 6% | serialization, file count | medium |

Barrier medians do not add to median total. Profile total median 1038.1 ms, mean 1079.2, sd 155.0, min 923.8/max 1273.5. CPU median user 1567.3/system 377.9 ms, peak RSS1,119,028KiB, sampled average 786,611KiB. Baseline no-profile total median 1389.97 ms, mean 1438.11, sd 526.13, min 842.69/max 2098.58 (unstable shared VM; not an official score). Diagnostics alter scheduling. Instrumentation-disabled candidate measured 1144.91 ms median in same interleaved comparison, but NOT evidence of a real gain: variance too large and code layout changed. All raw runs retained.

Diagnostic task CPU means per run: parse_events 840.6 ms, links 53.9 ms (includes waiting for nested work), dedup 315.1 ms, click parse 202.0 ms, session partitions 414.1 ms. Session gather 51.2 ms, grouping 63.9 ms are INCLUDED in session partitions. Arrow construction 36.9 ms and parquet 85.1 ms spread over workers. Heavy sorts 17.1 ms summed CPU/run across 5 whales; median whale 3.26 ms, maximum 5.87 ms, max 90,032 events. Largest person partition 93,844 events. No evidence whale serialization dominates at 5M. Session partition task median 0.32 ms/max 12.13 ms across runs. Thread CPU is attributable; process CPU spans overlap and cannot be summed. Worker indices and task end/wall permit reconstruction of utilization/scheduling.

Sequential diagnostic median 1011.3 ms (3 runs), with links 53.9/event 409.4/dedup 146.8/clicks 93.6/session 242.6/output 89.6 ms barrier deltas. It changes scheduling and is not substituted for normal mode.

Physical IO estimate: 7 cached AVX2 XOR scans over all input CSV bytes give median 7.614 GB/s; 7 buffered writes of the actual 26,196,836 output bytes give median 1.589 GB/s. Read 63.0 ms + write 16.5 ms =79.5 ms sequential IO-only budget at these *observed* rates; >=63 ms if fully overlapped. This is an empirical bandwidth estimate, not a rigorous floor: cache conditions and concurrent work differ, and official hardware/storage are unavailable. At proportional 20M volume, about 318 ms sequential at the same rates (not a predicted score). Warm baseline major faults 0; no cache-miss or stall data because perf_event_open returned EPERM.

Priorities: guaranteed-valid ingest fast parser (low risk), bounded persistent aggregation workers/output states (medium), lazy vs populated maps (low), compact exact-byte dedup identity (medium), cached-traffic reduction in dedup/scatter (high), whale radix sorting (medium risk, presently little measured headroom). Preserve dedup-before-validation and all timestamp boundaries.
