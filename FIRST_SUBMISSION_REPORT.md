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
baseline. Static Docker and 60M validation receipts are added separately only
after they finish.

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
