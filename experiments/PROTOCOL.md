# Experimental protocol

Frozen baseline executable built from pinned source, before instrumentation. Same Rust 1.100 stable, locked crates, release fat LTO and x86-64-v3 for all candidates. Local direct binaries initially dynamically linked; final Docker binary crt-static. Benchmarks run without builds/generation in parallel. Early dev timing recorded while compilation was running is invalid for comparison and superseded by isolated interleaved measurements.

`tools/bench.py` interleaves variants in forward/reverse order across repetitions, measures parent-observed start-to-exit with wait 4 (5 ms sampling/polling granularity), CPU user/system separately, max RSS, 5 ms sampled average RSS, faults, context switches, filesystem block IO, and output file bytes. Warm cache is default. --cold uses POSIX_FADV_DONTNEED on inputs: an advisory eviction, not a guaranteed machine reboot/cold disk. Output is recreated between runs. Logs have program-internal cumulative stage times; profile builds additionally emit noncumulative spans. Diagnostic timing reads/logging change scheduling; compare uninstrumented candidates for speed claims.

`SS_PROFILE=1` records per-task wall and thread CPU, process CPU overlaps, record and byte counts, worker index, allocation request counts/bytes, process resource usage. Parallel task durations are not additive critical-path wall times. `SS_SEQ=1` can isolate links vs parse and dedup vs clicks, but changes overlap, so must not be substituted for normal total time. Stage critical-path shares use deltas between existing barriers.

Local quota is 2 CPUs, 8 GiB; official 8 pinned hyperthreads/24 GB unavailable. Docker is rootless without cgroup control, thus limits cannot match official. No cache-miss/stall claims without perf access. Cached vector input scan and cached output write measure empirical IO ceilings; they do not establish an absolute physical lower bound on another machine.

Topology discovery: sysfs reports L2 shared_cpu_list=0-1 for CPUs 0/1 and 2 for CPU2 despite distinct virtual core IDs. Subsequent experiments pin child processes to CPUs 0,2 with --affinity 0,2. Calibration separately pinned 0,1 and was slower. This reduces a scheduling confound, but does not reproduce official 8-hyperthread topology. No application affinity hard-coding is added.

A late batch (event-keys-large) had first-run outliers 21.5 s and 54.9 s despite later 3.3–3.7 s runs. They are retained in raw data; no gain is attributed to their means. Final comparisons explicitly warm each variant once and reduce external RSS reads from 5 ms to 50 ms; wait 4 completion polling remains 5 ms. New JSON includes timestamped RSS samples for stage-level sampled memory estimates. Sampled stage peaks are not exact high-water marks and small stages may have no sample. Rusage supplies exact process lifetime max RSS. Allocation diagnostics count alloc/alloc_zeroed calls and their requested bytes; small realloc calls bypass these counters, so they are not a full live-allocation census.

Final harness revision: completion uses blocking wait 4 and a separate 50 ms RSS
sampler thread. It captures the end timestamp before joining the sampler, so
completion is no longer quantized by 5 ms polling. Final JSON explicitly records
wait_method=blocking_wait 4. Earlier measurements retain their original method.
