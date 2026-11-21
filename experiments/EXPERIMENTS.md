# Experiment ledger

Raw JSON and per-run timing logs are in `results/`; all timings below are local,
not official. Early batches used 5 ms RSS sampling; final batches use 50 ms sampling,
explicit warmups and affinity 0,2. See PROTOCOL.md. No build/generator runs overlap
the measured processes. Different rows use different batches: do not multiply
speedups or mix their absolute medians.

| Experiment | Control median ms | New median ms | Ratio control/new | Correctness | Decision |
|---|---:|---:|---:|---|---|
| Valid-F1 ingest specialization,5M/9 |869.10|897.62|0.968|fixtures/dev/ref/fuzz|No established total gain; provisional until ablated|
| Persistent worker states,5M/11 |916.85|874.10|1.049|fixtures/dev/ref/fuzz|Keep; fewer states/faults|
| Workers vs frozen baseline,20M/7 unpinned |3742.56|3424.67|1.093|same|Noisy, mean improves only 1.8%|
| Byte-exact event IDs,20M/7 unpinned |4124.21|3466.94|1.190|fixtures/dev/ref/fuzz|Keep for exactness; high variance|
| Circular prefetch queue,20M/7 pinned |3604.72|3535.81|1.019|subsequent full regressions|Keep|
|32-byte events,20M/7 |3397.11|3855.10|0.881|fixtures/dev/ref/fuzz|Reject: scattered CSV revisits|
|Dense 32-byte buffers,20M/5 |3907.22|3794.88|1.030|covered by overflow behavior|Reject 32-byte design; RSS down~750MiB|
|Dense 40-byte buffers,20M/7 |3393.95|3429.48|0.990|fixtures/dev/ref/fuzz|Memory improvement, time flat/noisy|
|Byte-check every map hit,20M/5 |3318.86|4752.59|0.698|unit collision/long UTF8,fixtures/dev/ref/fuzz/adversarial|Correct but slow; improve representation|
|Lossless short identity keys,20M/9 vs frozen |3552.22|3752.88|0.947|all above|Exactness recovered most overhead; not yet faster|
|Packed event keys,20M/7 |3412.42|3495.47|0.976|all above; out-of-range ingestion fix|Not yet faster vs frozen; first-run outliers 21 s/55 s retained|

Later experiments and final comparison appended after completion. `research.md`
records the hypotheses and interpretation, not just favorable medians. Canonical
expected outputs and generated data are never checked in.

## Completed later experiments

All rows refer to their own interleaved batch. Rejected configurations are not
submitted; no unmeasured compound speedup is inferred. Raw JSON includes mean,
SD and every sample. Final correctness is separately established on the exact
selected source and static Docker image.

| Experiment/raw JSON | Control | Candidate | n each | Control median ms | New median ms | Control/new | Decision |
|---|---|---|---:|---:|---:|---:|---|
| `event36-large.json` | event-keys | event 36 | 7 | 3512.36 | 3545.68 | 0.991 | Reject standalone layout gain; later 28 B design supersedes |
| `event36-largecap.json` | event 36 | event 36-largecap | 5 | 3460.13 | 3402.18 | 1.017 | Useful capacity result; superseded |
| `deferred-name-large.json` | event 36-largecap | deferred-name | 7 | 3467.28 | 3538.19 | 0.980 | Reject; no mean gain |
| `one-page-large.json` | event 36-largecap | one-page | 5 | 3462.18 | 3289.10 | 1.053 | Keep page-density idea |
| `p1-1024-large.json` | one-page | p1- 1024 | 5 | 3345.17 | 3442.68 | 0.972 | Reject for 36 B design |
| `p2-256-large.json` | p1- 1024 | p2- 256 | 7 | 3371.69 | 3413.65 | 0.988 | Reject |
| `encoded-persons-large.json` | baseline | encoded-persons | 7 | 3562.59 | 3300.66 | 1.079 | Keep exact compact representation |
| `encoded-p1-1024-large.json` | encoded-persons | encoded-p1- 1024 | 7 | 3208.85 | 3276.53 | 0.979 | Reject |
| `zero-gather-large.json` | encoded-p1- 1024 | zero-gather | 7 | 3357.63 | 3361.68 | 0.999 | Reject: pointer chasing offsets copy saving |
| `ingest-ablation-large.json` | generic-ingest | encoded-persons | 11 | 3412.94 | 3314.07 | 1.030 | Keep valid-F1 path; modest/noisy |
| `grouped8-large.json` | encoded-persons | grouped 8 | 7 | 3129.59 | 3174.33 | 0.986 | Reject |
| `click24-large.json` | grouped 8 | click 24 | 7 | 3231.44 | 3270.47 | 0.988 | Reject |
| `final-large.json` | baseline | final | 15 | 3543.21 | 3132.61 | 1.131 | SELECTED final source |
| `final-large-threads8.json` | baseline | final | 5 | 4056.03 | 3131.50 | 1.295 | SELECTED; VM quota still 2 |
| `docker-large.json` | baseline | final | 5 | 11077.48 | 5848.57 | 1.894 | Validation; large variance; actual affinity 0,1,2 |
