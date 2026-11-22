# Measured candidate report — Shopping Sessions / Codex

The selected candidate completed the local 20M comparison with exactly matching
user_daily and campaign_daily tables. **No official sub-0.4-second runtime has
been achieved.** This candidate's exact published scorer SHA is recorded in the
publication receipt; do not infer it from a moving branch name.

Frozen baseline: `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.
Previous validated source: `1ffcf309d044cf44d38ed49c071308b73b46118c`.
Inherited v11.3 architectural starting point: `0edb1e785c36ae85cdf53faddcfc319b7d424d48`.

The v11 architecture was already present in upstream when discovered. This
branch repairs its probabilistic equality, long-string truncation, pre-1970
ingestion ordering and uninitialized storage, and adds compact 16-byte event
and copy records with lossless uncommon-value fallbacks. It also adds optional
per-stage/task CPU, wall, record/byte and allocation diagnostics that compile
out of the default release.

Seven interleaved measured runs per variant, one warmup, 20M seed 2026,
1,883,811,848 CSV bytes, Xeon8573C VM, 2-CPU quota, 8GiB, affinity0,2.
Host noise affects these values. CPU is summed process execution, not wall.

|Variant|n|Median ms|Mean ms|SD ms|Min ms|Max ms|CPU ms|Peak RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|frozen|7|4210.20|4159.78|293.27|3846.57|4665.76|8040.03|4.07|
|prior|7|3628.14|3749.42|228.94|3565.31|4088.59|6712.40|3.15|
|selected|7|2659.28|2617.52|119.50|2406.32|2752.17|4961.98|2.89|

The selected median is 36.8% below the frozen baseline and 26.7% below the
previous candidate in this batch. Much of that gain comes from the inherited
v11 architecture. Record compaction's isolated median gain was 4.1% in one
nine-pair batch, with weaker evidence in a noisier repeat. Every sample is in
`experiments/results/sub400-selected-v11.json`; separate batch ratios must
not be multiplied.

Validation on the selected direct binary: 18 release units; 36/36 official
fixtures; exact official 1M development answer; literal-reference small input
plus four fuzz mutations; long UTF-8, signed-ingestion and boundary adversarial
cases; exact both-table comparison of the complete 20M input against the frozen
baseline. Fresh static Docker and exact 60M validation subsequently passed on a public runner. See the measured receipt below.

Earlier-source 60M evidence is in `experiments/results/hosted-result.json`:
old `1ffcf309` passed exact comparisons and Docker sandbox checks at 20M/60M,
with 60M medians 5553.3/5233.9ms, three pairs, 4-CPU AMD EPYC9V45 public runner.
Those numbers do not describe the selected source.

`experiments/SUB400_PROGRESS.md` contains the architecture assessment,
controlled experiment table, instruction profile, empirical IO-only estimate
and next priorities. Paired timestamp SIMD and a conservative linked-ID filter
passed correctness but failed performance and are excluded. A user cache was
rejected from measured reuse rates before implementation.

Official submission remains pending because the supplied browser tool failed
to initialize, so an authenticated official interface could not be reached.
No official submission allowance was consumed, and no rank or runtime is
inferred from GitHub publication or CI success. The deadline is
2026-11-22 16:00 UTC. The official scorer requires the public repository URL
and the exact candidate commit, with the root Dockerfile.

## Fresh Docker and 60M receipt

Validated exact source commit: **fef07368caf2e161fbf91dbf67e8176574fcfaa9**.
[Hosted run](https://github.com/akshayagg18/shopping-cl/actions/runs/44940061203),
Check: `Codex measured 20M/60M validation`. Both sources use the same pinned
Rust/static Docker configuration. Public AMD EPYC9V45,4 CPUs,16GiB,no swap.
The candidate image built in 10.36s; the baseline in 79.03s. Both complete
output tables,36 fixtures,development answer and static sandbox checks passed.

|Input|n each|Baseline median ms|Candidate median ms|Candidate mean / SD / min / max ms|
|---|---:|---:|---:|---|
|20M|7|1550.86|935.15|936.97 / 9.94 / 924.40 / 949.37|
|60M|3|4816.35|2857.89|2927.28 / 145.25 / 2829.73 / 3094.21|

That is 39.7% and 40.7% less median elapsed time respectively. Peak RSS was
3,043,284 versus4,280,796KiB at20M and8,996,996 versus13,324,256KiB at60M.
These establish an independent sub-second 20M result, not an official score
and not the requested sub-0.4 result. Full samples and hardware are in
`experiments/results/v11-hosted-result.json` and `submission/READY.json`.

## Current stage profile

Three opt-in diagnostic runs on the local2-CPU VM,20Mseed2026. Diagnostic
median2813.5ms versus normal2786.8ms in the same three-pair batch (~1% overhead).
Default release diagnostics compile out. Stage medians are separate envelopes;
nested concurrent task times must not be summed.

|Stage|Median wall ms|Process CPU ms|Share of stage envelope medians|Suspected cost / potential|
|---|---:|---:|---:|---|
|Open|1.24|1.77|0.04%|Lazy mapping; faults are charged during scans|
|Links / lookup tables|220.20|363.44|7.86%|Endpoint sorting, union-find and person map construction|
|Event and click parsing|1750.73|3313.68|62.50%|CSV/semantic instructions and stream bookkeeping; primary target|
|Deduplication|134.09|256.76|4.79%|Copy gathering and hash probes; limited total potential|
|Session grouping / aggregation|547.35|1089.98|19.54%|Person/bin grouping and click sorting; secondary target|
|Parquet output|112.94|192.38|4.03%|Column serialization and79.2MB of writes|
|Release|34.67|35.92|1.24%|Page-table and buffer reclamation|

Largest session task50.39ms,largest partition455,095 records; median task0.797ms.
This is observable skew but not the dominant2-CPU critical path. Parsing tasks
processed26,152,802 parseable event copies plus valid clicks; dedup records were
21,113,660. Allocation diagnostics recorded12,376 calls and1,653,746,598 requested
bytes (small system reallocations bypass counters). Sampled average/peak RSS,
worker CPU and logical byte counts are in `sub400-stage-summary.json`. Neither
requested bytes nor logical bytes are hardware memory traffic.

AVX-512 separator compaction passed correctness but did not show a consistent
paired gain; its isolated field scanner regressed. It is excluded. PGO also
passed correctness but its nine-pair median2591.2ms versus2567.2ms did not win.
The next structural experiment appends records contiguously before one stable
partition scatter, to reduce random parser bookkeeping. It remains a prototype.

## Dense candidate: correctness passed, final performance gate failed

Exact source `6308907ca3e9f556e0292631a5059cf6c5fe1ada` independently implements
runtime-learned exact placement, literal global identity resolution, inline-user
records and person-run grouping. Local nine-pair20M improvements repeated at
13.1% and17.5%. Fresh pinned static Docker built in14.30s;36 fixtures,development
answer,and both full20M/60M tables passed,including the restricted sandbox.

The fresh same-host comparison exposed a final-size regression. Intel Xeon
6975P-C,two physical cores/four SMT threads,16GiB,no swap. Both priors and the
candidate used identical pinned compiler/build flags. Seed20261123 at60M is
new relative to the earlier validation. **This source is not recommended for
final submission until the regression is understood and fixed.**

|Input|n each|Original D2 median ms|Prior fef0736 median ms|Dense median ms|Dense mean / SD / min / max ms|
|---|---:|---:|---:|---:|---|
|20M|7|1772.94|1299.58|1202.60|1207.49 /13.78 /1196.95 /1237.84|
|60M|3|5666.44|4168.96|8089.83|8000.35 /404.18 /7558.93 /8352.29|

At60M,candidate median system CPU was6123ms versus807ms for the prior,and user
CPU21285ms versus14589ms. Peak RSS was8,809,220 versus8,986,452KiB. This does not
support a simple higher-peak-memory explanation. A same-artifact diagnosis is
collecting stage envelopes,faults,context switches,and controls for huge pages,
stream layouts and click grouping across both60M seeds. No speculative cause
is presented as established. The general14-day generator window fits the
current timestamp range,so the initial range-overflow hypothesis is unproven.

Evidence: https://github.com/akshayagg18/shopping-cl/actions/runs/44967589723 ;
Check `Codex dense 20M/60M validation` at6308907. Complete measurements are in
`experiments/results/dense-hosted-result.json`. The earlier935ms result used a
different AMD runner; do not compare those absolute times as an improvement.
No official submission or sub-0.4 result has been established.
