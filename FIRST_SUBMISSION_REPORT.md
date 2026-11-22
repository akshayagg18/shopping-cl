# Shopping Sessions — measured current-generator candidate

**Sub-0.4 seconds is unproven. No official submission or score is claimed.**

Selected runtime source: `82d185b15affaf4b65fcb4936d23d410478debfc`.
Public repository: https://github.com/akshayagg18/shopping-cl .
Frozen starting baseline: `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.
The original commit and subsequent experimental branches remain preserved.

## What was implemented

The original D2 source already used SIMD CSV scanning, custom hash tables,
parallel dedup/session partitions, a huge-page allocator and Arrow/Parquet.
The subsequent v11.3 architecture was already in this user-owned repository
when discovered; this work does not claim authorship of that inherited design.
It resolves links first, writes candidates directly to person streams and
separate small event-ID copy streams, then marks losing copies dead. Half-hour
bin extrema replace per-event timestamp sorting, while clicks retain exact
(timestamp, byte ID) sorting. A direct standards-compliant Parquet writer
replaces the former Arrow path. See `research.md` and
`experiments/SUB400_PROGRESS.md` for the original complete source assessment.

This branch repairs probabilistic identifier equality, signed ingestion order,
long-name/reference truncation and uninitialized storage in that architecture.
Short IDs use injective literal encodings; other IDs use exact full-byte
interning. Opaque event IDs normally use checked source offset/length references
with full-byte equality after any hash match. Unrepresentable IDs, signed/wide
times and large stream locations have exact side-table fallbacks. No keyed
generator permutation is inverted or assumed. Mappings remain live through
deduplication. Forced-hash-collision tests cover different event IDs and an
earliest invalid copy; neither fingerprints nor hashes determine equality.

Current-generator improvements include 64 sharded fallback dictionaries,
per-worker exact user-ID caches, and source-reference event deduplication.
The selected combined source adds dedup source-byte prefetch 16 records ahead,
scanner inlining, 8-byte grouped event bodies/16-byte grouped click bodies,
and caps automatic person/dedup partitions at1,024. Environment overrides remain
available for controlled experiments. All transformation boundaries, byte
ordering, invalid-copy suppression and DATE output are unchanged.

## Dataset revision and reproducibility

The live rules/generator changed during this task. Current rules require opaque
variable-width IDs; one eighth of IDs has a32-character suffix. Earlier
935ms/20M and2,858ms/60M results used the previous generator and do not establish
current performance. Archived reports are retained with that limitation.

Current generator ZIP SHA256:
`78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a`.
Current development-answer SHA256:
`c2439e0e2e7456d721852814e9928e077648803d5c57a1760a40c7d3b0864d20`.
The hosted driver asserts these hashes, preserves every dataset key and runs
variants interleaved with alternating order. It records every sample, not only
successful-looking minima. Generated datasets are never committed.

## Fresh Docker and final-size validation

[Independent hosted run](https://github.com/akshayagg18/shopping-cl/actions/runs/45019804680).
Exposed EPYC9V74 topology:2 cores/4 SMT logical CPUs,16GiB,no swap. Every source
built fresh using the same pinned Rust/static Linux/AMD64 Docker configuration.
The selected build took13.06s. Its static binary SHA256 is
`6d237aff1f71626ae6babb7766ed408e20acdfe40eabbdff4ea9635a446a576b`.
The complete machine-readable receipt is `experiments/results/final-hosted-result.json`.

|Input / seed|CSV bytes|n|Prior exact ae0 median ms|Selected82 median ms|Mean / SD / min / max ms|
|---|---:|---:|---:|---:|---|
|20M /20261123|2,033,398,100|7|1670.63|1584.56|1568.10 /25.22 /1539.88 /1593.24|
|60M /20261123|6,094,665,387|3|5574.29|5039.23|5054.95 /27.83 /5038.54 /5087.08|
|60M /20261124|6,230,703,912|3|5688.47|5240.35|5253.03 /40.18 /5220.71 /5298.01|

These are5.2%,9.6% and7.9% less elapsed time than the previous validated exact
source on this host. At20M summed process user/system CPU medians were5768.78/
210.78ms and peak RSS3,275,756KiB. At60M peak RSS was9,588,108 and9,789,652KiB.
CPU execution across logical CPUs is not elapsed time.

Both complete output tables matched at every size. All three images passed
36 official fixtures and the current1M development answer. Thirty fresh
container runs additionally checked current1M/20M/two60M seeds, using a read-only
root filesystem, UID1000, no network,14GiB/no swap, and mounted /data and /output.
At20M selected fresh-container internal elapsed median was1527.7ms(n3), versus
Docker CLI wall2039.7ms. CLI wall includes daemon startup and cleanup; neither
is the organizer's official timing. The official hardware has8 pinned logical
CPUs and24GB, so these four-logical-CPU CI times must not be relabeled as scores.

An earlier corrected current-generator hosted run compared ae0 directly with
frozen D2:1682.74 versus1981.93ms at20M;5399.00 versus5872.20ms and5375.52 versus
5958.23ms at60M. [Receipt](https://github.com/akshayagg18/shopping-cl/actions/runs/44995086022).
These are separate hosts/batches; their ratios are not multiplied.

## Measured stage breakdown

Selected20M hosted barrier differences, median across7 runs. Shares normalize
these stage envelopes; their median sum differs from the median total.

|Stage|Median ms|Share|Measured cost / opportunity|
|---|---:|---:|---|
|Input open/map|1.4|0.1%|Lazy mappings; faults are charged to scans|
|Links and lookup construction|139.8|8.9%|Endpoint ordering, union-find and exact-ID construction; medium potential|
|Events/clicks parse and partition|1022.5|65.4%|CSV/semantic instructions and stream staging; primary opportunity|
|Deduplication|107.5|6.9%|Gather, source-ID hashing/probing, dead flags; prefetch already cuts much of this|
|Sessions and aggregation|227.4|14.6%|Person/bin grouping, click sorting, counters; compact bodies reduce traffic|
|Parquet output|50.5|3.2%|Column serialization and file writes; small total upside|
|Release|13.6|0.9%|Page tables and buffer reclamation|

Local opt-in diagnostic runs before the combination found a largest session
bucket of65ms and452,897 records, with balanced worker CPU. Whales cause skew
but were not the dominant local critical path. This conclusion is workload and
hardware specific. Hardware perf counters are denied by the current VM;
optional wall/process/thread CPU, allocations, task sizes and RSS/fault/context
switch records are retained. Normal release diagnostics compile out.

## Experiments and rejected hypotheses

Every implemented variant below passed exact outputs before selection. Native
VM results are separate interleaved batches on a2-CPU quota Xeon8573C; distributions
and all outliers are retained in `experiments/results`. Do not compare absolute
medians across rows or add their percentage gains.

|Hypothesis / change|Control median ms|Variant median ms|Decision|
|---|---:|---:|---|
|Source references + exact worker cache,20M(n7), versus frozen D2|4941.34|3182.30|Keep;35.6% local decrease, current generator|
|Source-ID prefetch16,20M(n7)|3218.42|3107.71|Keep after full combined Docker/final-size checks|
|Copy records16→24 bytes with cached hash,20M(n7)|3111.86|3026.54|Not selected; parse traffic/RSS tradeoff versus prefetch|
|Reference every ID rather than literal encoding,20M(n7)|2955.90|2979.59|Reject|
|Trusted ingestion decoder,20M(n7)|3384.08|3517.70|Reject; no repeatable gain|
|Tagged separator cursor,20M(n7)|3424.78|3351.49|Not selected; small gain within observed variation|
|Owned per-worker byte-ID arena,20M(n7), versus unchanged source|3177.30|3131.76|Not selected;1.4%, prefetch similar, more memory|
|Discard event mapping PTEs after owned IDs,20M(n7), versus arena|3131.76|3207.08|Reject for runtime;RSS lower but filesystem cache remains|
|Inline scanner,20M(n7) cleaner repeat|3164.95|3079.86|Keep only after combined validation|
|Lightweight grouped records,20M(n7)|3164.95|3031.22|Keep only after combined validation|
|All selected changes together,20M(n9)|3067.22|2964.77|Keep;3.3% local improvement in this batch|
|Extra literal stamp fast path, hosted60M seeds(n3)|5039.23 /5240.35|5132.07 /5286.49|Reject from final selection despite1%20M median win|
|Parser64KiB/256B guard versus192KiB/2048B,20M(n7)|2909.46|2856.39|Not selected;1.8%, SD111–148ms|

A hosted partition/worker matrix additionally tested256–4096 partitions and
2/4/8 workers, with exact20M and two60M outputs. More workers and fewer buckets
did not consistently win. Four workers were fastest on the four-logical-CPU
host; the scorer default remains8 workers for its8 logical CPUs. Automatic
partition capping is a modest final-size improvement, not a claimed large gain.

Local regression coverage includes22 release units,36 official fixtures,
legacy and current development answers, independent literal-reference small
seeds and four dirty-input fuzz rounds, full20M exact comparisons, and targeted
long UTF-8/80KB event IDs, signed ingestion, hash collisions and scanner-boundary
cases. The official Polars starter contains an empty transform; no independent
Polars rerun is claimed. The literal Python reference is independent.

## IO feasibility and remaining priorities

The current local20M input is2.033GB; the candidate's output is about86MB.
An actual full-byte AVX2 single-worker warm input probe had median315.9ms
(~6.44GB/s); a cache-drop-hinted scan had median1183.5ms. A single-worker write
probe used104.36MB of frozen-baseline Parquet and took168ms, without fsync.
Those measured costs are IO-only references, not strict physical lower bounds:
the pipeline scans/writes in parallel, warm memory bandwidth differs from disk
bandwidth, and input/output work can overlap. Adding these reference medians
would overstate an unavoidable floor. Raw bytes/counters are retained in
`experiments/results/opaque-io-20m.json`. The current runner cannot establish a
physical minimum for the official hardware or prove that400ms is achievable.

Next: statistically sample the actual hot parser instructions, then evaluate
one measured structural change. Repeated timestamp/field work and staging
cache pressure remain higher priorities than rewriting the already-small
Parquet stage. Recheck final-size latency and memory before any further source
selection. An optional targeted per-thread CPU sampler is experimental only;
normal submission source has no signal profiler.

## Submission status

Official submission remains pending secure Buildathon sign-in. No submission
or rank is inferred from pushing GitHub or passing CI. The public leaderboard
at12:10UTC showed a fastest practice score of0.4218s. The deadline is
2026-11-22 16:00UTC. Submit the public repository plus the exact selected SHA,
with the root Dockerfile; check the real interface's remaining allowance first.

Source tree is below50MB and includes no generated competition datasets.
The static image needs no runtime network. MIT deliverable; dependency licenses
are disclosed in README. Build correctness and local/hosted speedups are proven;
**the requested official sub-0.4-second result is not.**
