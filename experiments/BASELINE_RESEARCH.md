Historical report from the earlier 1ffcf309 investigation; later reports and hosted receipts supersede its pending-validation limitations.

# Shopping Sessions investigation

Baseline: d 2dc 441e 6bed 67a 57572a 7c 1f 12b 06dad 7f 163cc, frozen executable in ignored experiments/bin/baseline.

Authoritative sources read 2026-11-21 UTC:
- https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md
- https://www.joinbuilders.org/buildathon/tracks/shopping-sessions
- https://www.joinbuilders.org/buildathon/status
- Official generator Rust source and starter-polars fixtures/comparison tools (downloaded, ignored).

Official: linux/amd64, 8 pinned hyperthreads, 24 GB, no swap/network, uid 1000, read-only root; /tmp 64 MB, /scratch + /output 50 GB; image <=8 GB, network build <=10 minutes. 20M practice; 60M final; start-to-exit. Latest page: top 10 retimed 7 times alone, superseding briefing's top 5/5. Two submissions/hour. Score variation across machines up to 20%. Leader page observed 0.36 s, not a locally verified benchmark.

Local: Xeon Platinum 8573C VM, 3 visible vCPUs but cgroup cpu.max=200000/100000 (2 cores), memory.max=8589934592 (8 GiB). These are NOT official resources. Docker rootless vfs, no cgroup enforcement. Local experiments can compare architectures; absolute official score cannot be inferred. No existing shared-knowledge.md was present.

## Source assessment before changes

Read all src/{main,parse,alloc,output}.rs, Cargo.toml/lock/config, Dockerfile, README, and tools/{check,fuzz,ref}.py. Main uses populated mmaps; immutable byte references; linked-ID hash interning and serial union-find; simultaneous link construction/event parsing; pre-validation represented as INVALID but all parseable copies retained for earliest-ingest selection (semantically appropriate); 512 dedup buckets; person-map prefetch queue; 1024 counting-sort person partitions; local hash grouping; comparison sorting; two-pointer attribution and distinct-campaign counts; Arrow uncompressed Parquet. Explicit click dedup unnecessary under identical-copy contract.

Memory flow per event: source CSV -> 40-byte Ev1 -> dedup gather of Ev1 -> 24-byte Ev2 scratch -> by_bucket Ev2 -> session gather Ev2 -> local counting-sort Ev2 -> per-person comparison sort. EvChunk allocates based on a conservative 60-byte line bound plus 512*16 slack per CSV chunk, not actual record count. Dedup scratch and grouping hash tables scale with records rather than distinct IDs, making whale partitions oversized. Rayon fold can create more aggregation states/output files than workers. A whale still sorts serially within a bucket. Event timestamp sort does not need stability/tie IDs. Click ties do require byte comparison.

## Correctness risk audit (baseline)

- U64Map compares hashes, not identifier bytes; product/campaign/user collisions silently alias. Unlinked person key discards one hash bit. Event identity uses 96-bit hash, not bytes. This is probabilistic, not byte-exact under deliberate collisions.
- Vec::set_len on uninitialized Ev1/Ev2/Ck/Copy generic storage exposes invalid initialized integer elements; use MaybeUninit before claiming Rust soundness.
- Packed refs silently truncate fields >65535 bytes, files >64 GiB offsets, and file IDs >=4096. Truncated UTF-8 can make from_utf 8_unchecked invalid. Generator workloads are far smaller, but dialect places no explicit length limit.
- Relative ts u32 requires <49.7-day window (assert currently after parse). Ingest seconds clamped to u32 epoch (1970..2106), safe only for stated generator window 2025..2028. Counts u32 safe at 60M, not arbitrary sizes.
- F3 overflow returns early before checking remaining bytes; yields out-of-window sentinel on valid competition windows, so malformed vs outside doesn't affect output there.
- Category <255/campaign <65535 assertions rely on generator sizes. Window-relative days and revenues use sufficiently wide integers for stated workload.

No speculative score claim, no submission before correctness and Docker validation.

## Performance-engineering references consulted

- Langdale/Lemire, Parsing Gigabytes of JSON per Second (https://arxiv.org/abs/1902.08318): bitmap front ends reduce branchy structural scanning; supports measuring the existing two-stage approach rather than replacing it by reputation.
- Rust slice::sort_unstable docs (https://doc.rust-lang.org/std/primitive.slice.html) and https://github.com/Voultapher/sort-research-rs: current ipnsort is already adaptive for equal keys and structured inputs; radix wins must be established on integer timestamps with skew, not assumed.
- Linux transparent huge-page documentation (https://www.kernel.org/doc/html/latest/admin-guide/mm/transhuge.html): MADV_HUGEPAGE is advisory and fault/defrag behavior depends on host policy. Do not transplant allocator benefits between machines without measuring faults.

Hardware counters: a direct perf_event_open probe for user-space CPU cycles returned EPERM; hardware cache/stall/flamegraph attribution unavailable in this sandbox. No perf-based claims are made.

## Experiment 1: guaranteed-valid ingest parser

Hypothesis: validating guaranteed-valid F1 ingest values wastes parser CPU. Added a dedicated calendar conversion, preserving ts parsing/validation. 36 fixtures, dev expected, small literal reference +4 fuzz mutations pass. 9 interleaved 5M runs: control instrumented-disabled median 869.10 ms/mean 986.85/sd 213.94/min 804.35/max 1422.95; fast ingest median 897.62/mean 1025.54/sd 324.85/min 809.87/max 1810.12. Frozen baseline 1100.56 ms median in this batch; high variance and differing code layout make that insufficient for attributing a gain to ingest. Parser barrier median 385.9 ->373.3 ms, but total worse. Decision: provisional only, not established improvement; test independently against final worker configuration and remove unless robust.

## Experiment 2: bounded persistent aggregation workers

Changed only session/output scheduling relative to ingest candidate: exactly NCPU worker jobs dynamically acquire next partition with an atomic counter; each keeps one Agg/Scratch, then writes one user file. No person is split across workers; session/click ordering and category reduction unchanged. 36 fixtures/dev/reference/fuzz pass. 11 interleaved 5M runs: control median 916.85 ms, mean 1042.82, sd 280.21, min 828.90/max 1799.36; persistent median 874.10, mean 985.54, sd 234.40, min 785.21/max 1468.82. Median 4.66%/mean 5.49% faster, but noisy. Session barrier 161.6 ->143.1 ms; output 73.1 ->66.1 ms. Minor faults 54046 ->45252; exactly 2 user files instead of variable 16-33 states. Peak RSS unchanged because parsing dominates memory. Keep provisionally and confirm at 20M.

## Experiments 3–6: event identity, ring, compact records, buffer density

- Exact event IDs: replace 96-bit identity fingerprint with original-byte reference and one 32-bit probe tag. The first hash still chooses 512 partitions; matching tags compare full byte strings. Removes second per-copy seeded hash. 36 fixtures/dev/reference/fuzz pass. Unpinned 20M workers median 4124.21 ms -> exact 3466.94 ms, mean 4016.17 ->3720.62, sd 708.60/718.20: too noisy for a strong speed attribution. Retain exact byte semantics. Fixed-affinity 5M frozen baseline 898.92 vs exact 855.36 ms (5 runs), baseline sd 271.24 vs candidate 63.00; establish final evidence with longer paired runs.
- Ring prefetch queue: replace shift of 15 pending u32 indices per valid event with circular 16-slot FIFO, preserving emission order. Pinned 20M (7 runs), exact median 3604.72/mean 3658.65/sd 253.56 -> ring 3535.81/3489.57/99.04 ms. Median 1.91% and mean 4.62% improvement. Keep.
- Compact 32-byte Ev1: remove cached user hash and recompute from mmap after dedup. Pinned 20M (7 runs), ring 3397.11/3533.43/sd 424.44 -> compact 3855.10/3869.18/sd 55.26 ms. 13.5% median regression despite saving copies, because scattered CSV revisits hurt. REJECT.
- Compact buffer density: original capacity heuristic creates 3.05 MiB of 32-byte records per 4 MiB chunk, rounded to 4 MiB huge allocation. Smaller /90+24 estimate stays below 2 MiB; overflow vectors retain correctness. Five 20M runs compact 3907.22 ms -> compact-cap 3794.88 ms, RSS4235 MB ->3464MB(KiB measurements in raw data); still slower than 40-byte ring 3415.06 ms. REJECT32-byte design; useful memory insight.
- 40-byte dense buffers: /100+18 stays below 2 MiB per 4 MiB chunk, overflow protects short/skewed inputs. Seven 20M runs ring median 3393.95/mean 3645.31/sd 455.64 -> dense 3429.48/3430.42/69.24 ms. Time effectively flat/noisy, RSS4268.9MiB-equivalent KiB ->3551.7 thousandKiB (about 700 MiB less). Keep for final-size headroom, not as a proven speed win. 36 fixtures/dev/reference/fuzz pass.

## Correctness hardening under test

Byte-checked identifier maps; byte-checked unlinked grouping (63-bit key collisions); rare overflow references replacing 16-bit length truncation; MaybeUninit for partially-filled slabs; raw-pointer scatter writes followed by set_len only after full initialization. Targeted forced hash collisions and invalid-earliest duplicate exercise the rules rather than relying on collision probability. Large UTF-8 fields crossing 65,535 bytes exercise lossless references. No new arbitrary identifier restrictions.

Next hypothesis: encode short ASCII identifiers injectively with length tags (60 bits) rather than probabilistic hashes.9-byte u/U-prefixed identifiers get distinct tags; all other UTF-8/long IDs use a tagged hash PLUS byte comparisons. BMI2 PEXT packs 7-bit ASCII bytes; x86-64-v3 guarantees this instruction, and a scalar equivalent exists. This can restore exactness without loading the original user string on every common lookup. No dataset value, window or seed is hard-coded.

## Final assessment and ranked next work

The selected implementation uses exact reversible short-ID keys, full byte checks
for hashed fallbacks, exact unlinked interning,28-byte parsed events and 16-byte
surviving events. It retains 512 event buckets,1,024 person buckets,4 MiB chunks,
192KiB tokenizer sub-blocks and the original uncompressed Arrow/Parquet path.
The final stage profile and 15-run paired comparison are in
FIRST_SUBMISSION_REPORT.md. Original benchmark source remains untouched at the
pinned commit; final static image and a static baseline compiled with the same
Rust 1.100.0 builder both ran without networking.

| Priority | Opportunity | Measured available share / limit | Implementation risk | Next action |
|---|---|---|---|---|
|1|CSV/timestamp/instruction cost|Links/event parsing~42% of critical path; process CPU~2.63 s in a 1.34 s parallel envelope|High: exact dialect, calendars, invalid earliest copies|Profile on scorer hardware; benchmark timestamp/vector parsing separately before replacement|
|2|Dedup/click locality|~30% critical path; copied-reference gathering and extra partitions failed|High: dedup precedes validation, ID equality|Investigate dependent lookup/partition locality, not just total byte counts|
|3|Arrow/Parquet construction|~244 ms/8% wall; Arrow construction~74 ms per large user file|Medium: nullable strings and DATE logical type|Measure direct column construction/custom encoding only with exact reader checks|
|4|Whale scheduling|Largest partition 31.2 ms; largest sort 18.3 ms; not dominant on 2 CPUs|Medium: preserve whole-person ordering|Revisit heavy-first work ordering on real 8-worker hardware and 60M|
|5|Mapping/thread/chunk tuning|Warm open~50 ms; lazy mapping did not help;1/2/3/8 threads tested|Low to medium; hardware specific|Keep populated mappings and original chunk sizes until actual cache/topology evidence justifies tuning|

The two final grouped-record compactions also failed:8-byte grouped events
median 3174 ms vs 3129 ms control;24-byte grouped clicks 3270 ms vs 3231 ms control.
Neither is included. These are useful evidence that fewer bytes alone do not
predict speed on this workload. No full chunk-size grid, perf stall attribution,
or 60M end-to-end run is claimed.

Docker notes: default proxy trust failed inside the clean Rust image. An optional
BuildKit CA secret fixes local Cargo HTTPS while keeping verification enabled;
no CA is baked into the final image. Root build completed in 136.1 s and produced
a 6,302,024-byte linux/amd64 image. Static Docker passed 36 fixtures, the 1M answer,
and 20M equality against both the original static baseline and direct baseline.
Docker ignored requested cpuset 0,2 (static probe saw 0,1,2), and its container
measurements had high variance; they are execution evidence, not official score
predictions. The browser tool failed to initialize, so no authenticated official
submission was attempted and no practice allowance was used.
