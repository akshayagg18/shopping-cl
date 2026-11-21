# Hybrid sorting experiment (unscored)

Based on the user's 1.73-second reference commit `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.

Changes:
- For ordinary customers: retain standard timestamp sorting.
- For customers with >=192 events spanning >=8 half-hour buckets: counting scatter into 30-minute buckets, then sort within each bucket; this preserves exact global timestamp ordering with less comparison work on spread-out heavy customers.
- For click lists over 64 records: sort on timestamp only, and place the byte-wise smallest click ID first in each equal-timestamp group, preserving attribution ties.
- Add Rust unit tests exercising boundary timestamps and dense/sparse customer groups.

Independent C++ sorting microbenchmark (not an official benchmark): with events spread across 672 half-hour buckets, measured bucket-sort speedups ranged from 1.1x at 128 events to ~3.15x at 20,000; for 32 events it was slower. C++ proxy timings do not establish that the Rust scorer runtime improves.

Correctness: 40,000 randomized customer event sets compared bucket-boundary session reconstruction against a chronological sort, matching exactly. This does NOT validate the complete modified Rust executable; run Rust tests and official dev fixtures before scoring.

Follow-on idea, not yet implemented: replace per-session campaign-assist enumeration for whale customer groups with merged 7-day campaign coverage intervals; only adopt if an end-to-end benchmark wins.
