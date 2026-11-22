# Independent dense Rust experiments

This report records experiments after source `fef0736`. The inline-user, shared
run and grouped-click combination is now the default candidate. It is awaiting
fresh static Docker and60M validation. The scorer-ready source remains fef0736
until those checks succeed. No official sub-0.4 receipt exists.

Local tests: 20M seed2026, 1,883,811,848 CSV bytes, 2 CPU quota, affinity0,2.
Each batch interleaves variants, reverses order and includes one warmup.
The measured distributions below are start-to-exit; do not compare batches as
if the host were constant. Raw JSON also includes CPU time, RSS samples, faults,
context switches, I/O and individual stage logs.

|Batch|Variant|n|Median ms|Mean ms|SD ms|Min ms|Max ms|
|---|---|---:|---:|---:|---:|---:|---:|
|sub400-own-dense|dense|9|2402.55|2497.28|359.18|2274.30|3429.11|
|sub400-own-dense|users|9|2607.79|2602.97|58.54|2511.77|2705.61|
|sub400-own-dense|validated|9|2361.22|2369.46|58.62|2284.74|2446.94|
|sub400-local-csv|csv|9|2659.29|2685.36|107.10|2595.56|2891.21|
|sub400-local-csv|densecsv|9|2660.13|2809.22|352.47|2538.76|3663.37|
|sub400-local-csv|stock|9|2627.57|2666.19|166.96|2486.79|3040.62|
|sub400-dense-runs|runs|9|2745.64|2755.82|110.49|2593.48|2998.18|
|sub400-dense-runs|slots|9|2388.46|2410.39|94.41|2322.06|2604.28|
|sub400-dense-runs|validated|9|2395.46|2406.37|45.82|2340.26|2488.64|

## Changes and decisions

* Exact dense user graph: a runtime-learned reversible label map selects array
  indices, but only literal links establish components. Cross-group and unusual
  names use the same global union-find and exact dictionary. Canonical names
  compare full bytes. This alone regressed; default off.
* Direct event slots: 16-byte records with full signed-ingestion escape entries,
  exact label fallbacks and pre-validation invalid winners. Passed both full20M
  tables, all36 fixtures, official development answer, literal reference/fuzz and
  adversarial inputs. No overall gain by itself; default off.
* Line-local CSV: two 64-byte windows, quote-carry validation, scalar exact
  fallback. Exhaustive short quote strings,100K random masks, fragmented chunks,
  long fields, all36 fixtures, development answer, five fuzz rounds, adversarial
  cases and exact20M passed. Median1.2% slower in its batch; default off.
* Literal run descriptors: 19,485,372 surviving events compress into714,308
  descriptors, with arbitrary time order checked and corrected. Generic and dense
  classes merge before sessionization. An independent aggregate equivalence test
  includes fragmented runs, a large user, generic events and disordered times.
  All25 units and the full suite/20M passed, but median14.6% slower. Default off.
  A shared allocation and separate click grouping are being investigated before
  rejecting the descriptor architecture itself.

## Provenance constraint

Public research found a much faster MIT-licensed C++ implementation using dense
IDs and run processing. It was benchmarked locally only as an architectural
reference. None of that entry's code is in this source tree or the submission.
The general official rules say: "Your entry should be your team's own work.
Excessive reuse of code, another team's entry included, may result in
disqualification, at the organizers' discretion."
Source: https://www.joinbuilders.org/buildathon#rules . This Rust prototype is
independently implemented from the public generator's reversible ID format and
our existing exact code, with uniform literal-graph handling and general fallbacks.

## Best validated source

`fef07368caf2e161fbf91dbf67e8176574fcfaa9`, public branch codex/v11-exact.
Hosted 20M median935.15ms vs1550.86ms, seven pairs; hosted60M2857.89ms vs4816.35ms,
three pairs. Exact direct/Docker outputs; fresh pinned static Docker build.
Evidence: https://github.com/akshayagg18/shopping-cl/actions/runs/44940061203 .
Neither CI nor local timings are official scorer receipts.

## Selected inline-user combination

Nine interleaved measured20M runs plus one warmup per variant,2-CPU quota,
affinity0,2. All measurements are start-to-exit. Both complete output tables
matched the frozen baseline. The static Docker/60M gate remains pending.

|Variant|Median ms|Mean ms|SD ms|Min ms|Max ms|
|---|---:|---:|---:|---:|---:|
|public|2415.39|2513.38|211.44|2329.82|2964.07|
|clicks|2365.22|2399.54|184.12|2209.16|2781.97|
|side|2427.26|2477.31|155.91|2281.36|2707.85|
|inline|2098.98|2171.08|178.78|1968.61|2494.17|
|inline_scatter|2138.88|2208.35|252.65|1967.48|2831.29|

The selected median2098.98ms is13.1% below the published fef0736 control2415.39ms
in the same batch. This is a new local improvement, not an official score.
Inline24-byte records avoid the separate array's random user-ID fetches and
perform expensive identity lookup after dedup ordering improves its locality.
The128-entry cache checks complete injective payloads; collisions replace cache
entries and never merge identities. Dense label placement always has exact
fallbacks. Literal runs validate their grouping; time ordering is checked and
repaired; generic winners merge before sessionization. Default CSV parsing is
unchanged. No static dataset values, window, seed or expected outputs are stored.

The earlier six-way deferred-user batch was interrupted by an environment
restart in round7. Its summary uses all seven fully completed rounds and retains
the partial round separately. An empty output directory from the interrupted
process failed comparison; all complete variants were rerun and passed exact20M
comparison. This was an interrupted artifact, not a transformation mismatch.

Current verification:26 release units,36/36 official fixtures,official1M answer,
literal reference with five fuzz rounds,long UTF-8/boundary/pre-epoch adversarial
input,and full20M both-table comparison. Default flags are being checked again
before source publication. Unselected CSV,side-array and per-range-buffer modes
remain available only for controlled experiments.

## Independent repeat of the default configuration

Nine additional interleaved runs per variant,one warmup,20M seed2026,2 CPU,
affinity0,2. Default source passed26 units,36 fixtures,development answer,
small reference plus four fuzz mutations,and adversarial inputs without switches.

|Variant|Median ms|Mean ms|SD ms|Min ms|Max ms|
|---|---:|---:|---:|---:|---:|
|public|2704.67|2662.53|184.02|2419.96|2899.83|
|default|2230.02|2380.98|413.44|2056.24|3346.68|

This repeat is17.5% lower by median;the first batch was13.1%. Means,variance
and every sample are retained rather than discarding slow runs. Complete20M
output equality passed. No official timing or new60M/Docker proof is claimed yet.

## Fresh hosted gate

Fresh Docker and exact60M correctness passed for6308907,but final performance
failed. On the same Intel6975P-C four-logical-CPU runner,20M improved1299.58 to
1202.60ms while60M regressed4168.96 to8089.83ms. See FIRST_SUBMISSION_REPORT.md,
the dense-hosted-result.json receipt,and run44967589723. The recommended scorer
source remains fef0736. Local improvements are insufficient to select this
architecture for final scoring. Stage-level60M diagnosis is active.
