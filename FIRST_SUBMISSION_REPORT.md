# Current measured candidate — Shopping Sessions

**Validated source: ae0cf75dfbcc7eead8c68e20b09ad4ad6cb962c2.**
[Fresh Docker and current-generator validation](https://github.com/akshayagg18/shopping-cl/actions/runs/44995086022).
No official submission or runtime exists. **Sub-0.4 seconds is unproven.**
The user-reported1.73-second practice baseline has no scorer receipt in this workspace.

## Workload revision

The current generator is pinned to SHA256
78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a.
Its rules require opaque variable-width IDs, with a32-hex suffix on one in eight.
Earlier935ms evidence used another generator and hardware; it cannot establish
performance for this workload. Historical evidence is preserved in
[OLD_GENERATOR_REPORT.md](experiments/OLD_GENERATOR_REPORT.md).

## Implemented architecture

The inherited v11 pipeline resolves actual link edges first, parses directly into
person streams, and separately deduplicates compact copy records before admitting
survivors to sessions. Our exactness repairs retain injective short-ID encodings,
full-byte fallback equality, signed ingestion ordering, lossless long references,
initialized unsafe buffers, exact click ties, and standards-compliant DATE output.

The new opaque-ID path removes the global event dictionary from ordinary source
fields. A16-byte copy stores an exact source offset/length. Hashes choose buckets;
full bytes decide equality. Longer fields and unsuitable address spans fall back
to exact owned dictionaries. Worker-local full-byte maps cache shared user keys;
fallback dictionaries use64 cache-line-separated shards. No generator numbers
are decoded and no identity connections are inferred. See
[OPAQUE_IDS.md](experiments/OPAQUE_IDS.md) and source tests.

## Repeated local20M results

Current generator, seed20261123,20M events/5M clicks,2,033,398,100 CSV bytes.
Xeon8573C VM,2-CPU quota,8GiB,4 workers,affinity0,2; seven interleaved runs and
one warmup each. Timed combined source b4a239d has the same enabled data path
as ae0cf75 defaults; the default binary was separately compared exactly.

|Variant|Median ms|Mean ms|SD ms|Min ms|Max ms|
|---|---:|---:|---:|---:|---:|
|Frozen D2|4941.34|5322.42|810.84|4405.88|6415.55|
|Prior exact fef|5801.24|5800.90|212.55|5609.64|6222.86|
|Sharded fallback dictionaries|4926.71|4847.30|187.50|4521.54|5076.83|
|Event source references|3334.84|3335.03|30.08|3296.80|3389.71|
|References + worker user cache|3182.30|3211.21|93.02|3120.55|3383.75|

The combined median is35.6% below frozen D2 on this host. Both full tables match
D2 exactly for every variant. Raw samples, CPU/RSS/faults/context switches and
binary/asset hashes are in experiments/results/opaque-20m*.json.

|Stage|Median wall ms|Share of normal stage envelopes|Suspected cost|Potential|
|---|---:|---:|---|---|
|Open|1.3|0.04%|Lazy mappings; faults occur in scans|Low warm-cache|
|Links + dimensions|259.5|8.2%|Endpoint sorting, exact identity dictionaries, union-find|Moderate|
|Event/click parse + route|1976.0|62.8%|Tokenization, timestamps, ID handling, stream writes|Highest|
|Dedup|301.3|9.6%|Referenced-ID cache misses, copy gathering and probing|Moderate; tested prefetch|
|Sessions + aggregation|479.6|15.2%|Gathering, grouping and temporal aggregation|Moderate|
|Parquet|109.7|3.5%|Strings, encoding and regular-file writes|Limited total impact|
|Release|21.6|0.7%|Unmapping owned buffers and files|Low|

Three opt-in diagnostic runs measured3316ms median versus3169.7ms normal in
that batch (~4.6% overhead; one normal4852ms host outlier retained). Parse
process CPU was3798ms inside1913ms wall, dedup922 inside464, sessions1025
inside516, output211 inside113. CPU sums execution across threads. Nested
spans overlap and are never summed as elapsed time. Diagnostic sampled peak
RSS was3.13GiB,205,977 allocations/requested1.87GB. Largest session bucket65ms,
452,897 input records; session CPU was balanced across workers. Hardware perf
counters are denied here; no hardware cache-miss or IPC claims are made.

## Independent Docker/final-size evidence

Fresh static Linux/AMD64 build completed in16.07s. Image
sha256:5711299fd51b6a7b503be36f42f670abe7c591a3cf1ac5d13567865b08c901a4.
All36 fixtures, current1M development answer,20M and two60M seeds passed exact
comparison. UID1000/read-only root/no-network/14GiB Docker runs passed too.
Source/regression testing also passed22 release units, independent literal
references and four fuzz mutations, long UTF-8/ingestion/boundary adversarial
cases, concurrent dictionary insertion, and forced all-equal event hashes.

Hosted AMD EPYC7763,2 physical/4 SMT CPUs,16GiB,no swap. The retained final
60M seed20261124 has6,230,703,912 CSV bytes:

|Source|n|Median ms|Mean ms|SD ms|Min ms|Max ms|Peak RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|Frozen D2|3|7298.75|7200.34|420.87|6738.98|7563.29|13.72|
|Prior exact fef|3|16824.04|16685.59|258.05|16387.86|16844.85|10.20|
|Current ae0cf75|3|6965.62|7018.58|207.72|6842.47|7247.65|9.42|

Candidate process user/system CPU25269/771ms; prior36570/8313ms. This removes
the prior's long-ID lock collapse. Candidate median is4.6% below D2 here, with
substantial variance and only three samples. Do not extrapolate the larger local
relative gain across hardware. A reporter variable-shadowing bug overwrote the
20M and first60M timing summaries under a stage-name key. Their correctness
checks passed, but those timing distributions are unavailable in the retained
receipt. The corrected rerun completed and retained every study, as recorded below. Raw retained
receipt is experiments/results/opaque-hosted-result.json, keyed released;
its manifest unambiguously identifies the final seed. No missing numbers were
reconstructed or claimed.

## IO reference and next experiments

A complete AVX2 input scan read all2.033GB in315.9ms median warm (7 runs),
1183.5ms with cache-dropping hints (3 runs). Actual read-block counters are
retained. Writing preloaded104.36MB frozen-baseline output bytes into one regular file took
168.0ms median (7 runs), no fsync. These are single-worker IO-only reference
costs, not absolute hardware minima. Parallel writes can be faster; pipeline
output already is. The official8-hyperthread/24GB host differs from this VM.

Prefetching source IDs16 records ahead passed all20M comparisons and reduced
the paired median3218.4 to3107.7ms (3.4%); dedup371.4 to234.5ms. This remains
an experiment pending final-size Docker validation. Cached hashes in24-byte
copies cut dedup315 to133ms but raised parsing1910 to2006ms and RSS~160MiB;
net median gain was2.7%, similar to prefetch and noisier. Next tests evaluate
all-reference IDs and valid-F1 ingestion decoding, then select only measured
wins and retest unseen final-size seeds. Repository size remains below50MB;
no generated datasets are committed.

## Corrected independent receipt

The reporting fix preserved every dataset. Source remains exactly ae0cf75.
Fresh runner AMD EPYC9V45,2 physical/4 SMT CPUs,16GiB,no swap; pinned current
generator, compiler and static build. All direct and sandbox comparisons passed.

|Input / seed|n each|D2 median ms|Prior exact median ms|Candidate median ms|Candidate mean / SD / min / max ms|
|---|---:|---:|---:|---:|---|
|20m / 20261123|7|1981.93|4304.45|1682.74|1674.20 / 41.58 / 1590.45 / 1728.27|
|60m / 20261123|3|5872.20|15511.35|5399.00|5399.47 / 34.00 / 5365.72 / 5433.71|
|60m-fresh / 20261124|3|5958.23|14873.69|5375.52|5435.47 / 127.17 / 5349.36 / 5581.53|

The candidate median is15.1% below the frozen baseline at20M and8.1% /9.8%
below it on the two60M seeds on this host. These distributions are distinct from
the earlier EPYC7763 run and the local Xeon VM; they are not combined. Candidate
20M stage medians: links138.0ms, parse1043.6, dedup194.2, sessions219.3, output55.6,
release17.4. Final-size peak RSS was9.31 /9.43GiB, versus D2's13.48 /13.72GiB.
Full samples, hardware, manifests, asset/artifact hashes and resource counters:
`experiments/results/opaque-hosted-v2-result.json`. This is an independently
measured improvement, not an official score or a proven sub-0.4 result.
