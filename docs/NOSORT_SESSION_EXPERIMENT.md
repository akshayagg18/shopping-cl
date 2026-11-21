# No-sort sessionization experiment

For a person with at least 256 events, avoid sorting all event timestamps. Since each occupied interval of width 30 minutes contains timestamps at most 30 minutes apart, all its events belong to the same session. Determine the start and end of each occupied bucket; join successive occupied buckets exactly when next.min-prev.max <= 30 minutes. A second linear pass assigns every event to its session and aggregates purchases, categories and daily totals. Click ordering and credit/assist rules are unchanged.

For smaller persons, retain Claude's original chronological sort, avoiding per-person bucket array setup.

Independent Python equivalence test: 40,000 random/adversarial customer sets matched chronological sessionization exactly; repository also adds a Rust unit test comparing complete campaign and daily user aggregates against the original processor.

This is an unscored experimental variant. The GitHub workflow compiles with x86-64-v3/AVX2, checks the 1M dev-answer hashes, and benchmarks repeated whole-process runs against original Claude code. Do not submit unless benchmarks demonstrate an improvement.
