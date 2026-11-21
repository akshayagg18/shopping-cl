# First submission report — Shopping Sessions / Codex

Validated implementation prepared on 2026-11-21 UTC from pinned baseline `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. Validated scorer commit: `1ffcf309d044cf44d38ed49c071308b73b46118c` (see `submission/READY.json`). Original history is preserved on a distinct optimization branch.

**Local 20M result: 3.543 s → 3.133 s median, 11.6% less elapsed time.** With NCPU=8 on this same 2-CPU-quota VM: 4.056 s → 3.131 s, 22.8% faster. These are not official scores. No sub-1 s or sub-0.5 s official result is claimed.

## Environment and method

Xeon Platinum 8573C VM; 3 visible logical CPUs, 2-CPU cgroup quota,8 GiB RAM. CPUs 0/1 share L2; CPU 2 is separate. Final direct comparisons pin 0,2, interleave forward/reverse variant order, warm each variant once, use blocking `wait4` for start-to-exit time and 50 ms RSS sampling. No builds or generators overlap timed runs. Official resources are 8 pinned hyperthreads/24 GB and were unavailable. Docker is rootless vfs without cgroup enforcement. Hardware performance counters returned EPERM.

Practice baseline 1.73 s and rank 133 were user reports, not independently reproduced official results. Current live page observed leader 0.36 s. The page now specifies top 10 final entries retimed 7 times alone; this supersedes the briefing's top 5/5.

## Repeated direct measurements

All units below are milliseconds. Sample standard deviation; timings include process launch and exit.

| Dataset / condition | Variant | n | Median | Mean | SD | Min | Max |
|---|---|---:|---:|---:|---:|---:|---:|
| 20K / warm | baseline | 7 | 37.85 | 40.10 | 6.98 | 31.79 | 50.64 |
| 20K / warm | final | 7 | 29.28 | 29.44 | 2.29 | 27.22 | 34.18 |
| 1M / warm | baseline | 7 | 188.16 | 189.76 | 9.01 | 178.22 | 206.07 |
| 1M / warm | final | 7 | 172.81 | 171.20 | 4.78 | 165.39 | 178.14 |
| 5M / warm | baseline | 7 | 828.36 | 865.36 | 71.68 | 793.18 | 971.19 |
| 5M / warm | final | 7 | 793.51 | 786.20 | 25.66 | 753.07 | 818.78 |
| 5M / advisory cold | baseline | 3 | 1,028.40 | 1,110.44 | 199.34 | 965.21 | 1,337.71 |
| 5M / advisory cold | final | 3 | 914.53 | 921.32 | 14.72 | 911.22 | 938.21 |
| 20M / warm / NCPU=2 | baseline | 15 | 3,543.21 | 3,489.55 | 133.17 | 3,189.13 | 3,639.92 |
| 20M / warm / NCPU=2 | final | 15 | 3,132.61 | 3,156.38 | 110.73 | 3,008.81 | 3,430.41 |
| 20M / warm / NCPU=8 | baseline | 5 | 4,056.03 | 3,982.46 | 168.30 | 3,737.72 | 4,167.18 |
| 20M / warm / NCPU=8 | final | 5 | 3,131.50 | 3,122.39 | 51.04 | 3,062.40 | 3,190.47 |

Cold means POSIX_FADV_DONTNEED on owned input files before every run, not a reboot or a guaranteed cold storage device. Raw filesystem input blocks/fault counts are retained. NCPU=8 tests still share the VM's 2-CPU quota. Do not scale these values linearly to official hardware.

|20M process metric (median, NCPU=2) | Baseline | Final |
|---|---:|---:|
| User CPU ms | 5,414.940 | 5,114.583 |
| System CPU ms | 1,207.259 | 792.918 |
| Peak RSS GiB | 4.071 | 3.151 |
| Sampled average RSS GiB | 2.872 | 2.452 |
| Minor faults | 177,957.000 | 129,762.000 |
| Major faults | 0.000 | 0.000 |
| Output bytes | 97,043,249.000 | 97,024,108.000 |

Peak RSS falls by 22.6%. Mean runtime falls by 9.5%. CPU time is summed CPU, not elapsed wall time.

## Stage profile

Three additional opt-in diagnostic runs. Process CPU spans below are exclusive barrier envelopes; nested task spans overlap and are not added into elapsed time. RSS averages/maxima are sampled within the envelope; <2 samples is reported as n/a. Sampling timestamps have a small process-launch offset and 50 ms resolution.

| Stage | Median wall ms | Process CPU ms | Avg / sampled max RSS GiB | Records | Logical bytes |
|---|---:|---:|---|---:|---:|
| open | 49.90 | 51.07 | n/a | 111 | 1,883,811,848 |
| lookups | 13.60 | 13.62 | n/a | 52,000 | 736,690 |
| links_parse_phase | 1338.27 | 2625.95 | 2.212 / 2.620 | 21,113,660 | 1,642,755,503 |
| dedup_click_phase | 941.58 | 1867.52 | 2.903 / 3.151 | 26,152,802 | 831,502,135 |
| session_phase | 539.89 | 1073.74 | 2.465 / 2.510 | 24,524,616 | 473,020,128 |
| output | 243.85 | 469.66 | 2.576 / 2.645 | 2,224,281 | 97,031,818 |

Diagnostics count 62,911 alloc/alloc_zeroed calls and 2,774,140,901 requested bytes per run (median). Small reallocs bypass these counters; requested bytes are cumulative, not live memory. Open records are files, lookup records are dictionary entries, links/parse records are parseable event copies, dedup/click records combine event copies and valid clicks, session records combine surviving events and clicks, output records are user rows. Input parse bytes are raw CSV; later stages report logical representation bytes, not hardware memory traffic.

- dedup: task wall median 1.902 ms, max 15.312 ms; max records 42,674.
- session_bucket: task wall median 0.869 ms, max 31.195 ms; max records 394,271.
- heavy_sort: task wall median 0.287 ms, max 18.300 ms; max records 379,709.

Whale/skew partitions exist, but their individual sorts do not dominate the measured critical path. Synchronization is not separately resolved: wall/thread-CPU gaps also include descheduling. Actual 8-hyperthread hardware and 60M scaling still need official validation.

## IO floor estimate

The 20M seed 2026 dataset contains **1,883,811,848 CSV bytes**; output contains **97,031,818 bytes** (NCPU=8). Full CSV-content/name digest: `d953dedb755f3cff80eb09e4a353c5b8e141ec3e1a0c2813281095f7d2db5bc4`. Manifest: `{"window_start": "2026-04-15T00:00:00Z", "window_end": "2026-04-29T00:00:00Z"}`.

Seven cached AVX2 full scans measured median 6.637 GB/s; seven cached writes of real output bytes measured 1.679 GB/s. At these observed rates, reading alone takes about 283.8 ms and writing 57.8 ms: 341.6 ms serialized, or at least 283.8 ms if fully overlapped. This is an empirical IO-only estimate on this VM, not a proven physical floor or an official-machine prediction. mmap population/page-table setup and cache behavior are included in the scan. No compression/fsync assumption is substituted for the actual pipeline.

## Implemented design and correctness

Lossless short ASCII encodings carry exact IDs through the pipeline. Hash fallback paths compare complete bytes; unlinked fallback IDs receive exact interned keys. Parsed events are 28 B instead of 40 B, surviving events 16 B instead of 24 B. Names are decoded/referenced only at output. Bounded persistent workers reduce aggregation states/file overhead. The prefetch FIFO is circular, and slab estimates target one 2 MiB mapping while overflow vectors preserve correctness.

Original probabilistic identifier aliases are removed. Long refs no longer truncate at 65,535 bytes or 64 GiB offsets; partial buffers use MaybeUninit and scatter lengths are set only after initialization. Out-of-u32 ingestion dates retain full comparison data. New BMI2 use is gated by the target feature; x86-64-v3 already required BMI2/AVX2 in the baseline. Other UTF-8 IDs remain supported, not rejected.

Validation completed:

- 36/36 official fixtures for the frozen baseline and final implementation.
- Official seed-1 1M answer: both tables exactly equal.
- Literal Python reference on 20K seed 314 plus four supplied fuzz mutations; final run uses 8 workers.
- Targeted forced hash/fingerprint collisions,100K encoding round trips, long UTF-8 user/event IDs, pre-1970 ingestion selection, Unicode/unlinked names, missing final LF and parser block boundaries.
- 20M seed 2026: both final tables exactly equal frozen baseline outputs at NCPU=2 and NCPU=8.
- 3 release unit tests pass.
- Additional seeds 7,99,2028 (20K events/5K clicks each) match both literal-reference tables with 8 workers.

The repository's historical separate Polars reference was absent and is not claimed as rerun; the downloaded official Polars starter is empty.60M could not be validated on this 8 GiB VM; integer/count bounds were audited for 60M, but a full final-size run remains outstanding. The generator's 14-day/20-category/2,000-campaign limits are preserved.

## Experiments and next work

See [the complete ledger](experiments/EXPERIMENTS.md), [source assessment](research.md), [initial pre-change report](experiments/INITIAL_REPORT.md) and [protocol](experiments/PROTOCOL.md). Rejected designs include recomputing user hashes after dedup, unconditional byte checks on every common lookup, more partitions, borrowed-record gathering and extra grouped-record compaction.

Priorities after an official score: (1) profile on the actual 8-hyperthread/24 GB machine and validate 60M; (2) reduce timestamp/CSV parser instructions, still the largest phase; (3) measure record/table memory locality before further redistribution changes; (4) revisit heavy-first scheduling atthe official 8-worker configuration; (5) optimize Arrow construction/encoding only after measuring its share there. Sub-0.5 s has not been demonstrated and needs a substantially different throughput result, not extrapolation.

## Docker and submission status

Root Dockerfile build exit=0, elapsed=136.1 s. Local proxy trust supplied as an optional BuildKit CA secret; TLS verification remains enabled and no CA enters the image. The default public build uses its normal trust roots.
docker-dev-comparison.log: OK
docker-large-comparison.log: OK

| Docker 20M, NCPU=8 | n | Median ms | Mean | SD | Min | Max |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 5 | 11077.48 | 10236.10 | 2614.67 | 5721.33 | 12388.20 |
| final | 5 | 5848.57 | 6325.29 | 1516.76 | 4523.19 | 8134.96 |

Container timings are highly variable and not an official score estimate. Both images use the same pinned Rust 1.100.0 compiler, static flags and locked dependencies; baseline source is exactly the original commit. A static affinity probe proved Docker ignored requested cpuset 0,2: containers actually see CPUs 0,1,2. Hence Docker absolute times must not be compared with the direct 0,2-pinned runs. State start-to-exit excludes CLI create/attach overhead; CLI elapsed is retained separately. The rootless daemon cannot reproduce official resource controls. The optimized static image also passed all 36 official fixtures and the 1M answer;20M outputs match both frozen static Docker baseline and original direct baseline.

Public repository: https://github.com/akshayagg18/shopping-cl . No generated datasets are versioned. Root Dockerfile and lockfile provide a static linux/amd64 build; runtime has no network dependency. MIT notice and dependency/AI disclosures are included.

**Official submission not performed.** The browser tool failed to initialize, so no authenticated competition session was verified. No practice allowance was consumed; no official time, rank or validation receipt exists for this candidate. Submit the exact validated commit through https://www.joinbuilders.org/buildathon/tracks/shopping-sessions ; wait for actual scoring feedback. The cutoff is 2026-11-22 16:00 UTC (11:00AM EST).
