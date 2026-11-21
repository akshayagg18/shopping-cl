# Adaptive interval-assist experiment (unscored)

Based on Claude 1.73-second submission plus the hybrid half-hour bucket experiment.
For extremely large customers (>= 5000 surviving events and >= 1000 valid click records), the original algorithm loops over all active campaigns for each session. This variant instead merges the union of each campaign's inclusive seven-day click coverage intervals and counts qualifying session starts per session-start UTC day with binary searches. Credit determination uses the original latest-click sweep and byte-wise smallest click-id tie rule. Smaller customers preserve the original proven sliding-window method.

The equivalence of interval union with distinct campaign assistance follows directly from the published eligibility predicate. Unit tests compare to a direct enumeration reference including boundary clicks, duplicate clicks and multiple campaigns. The source already includes a verified original fallback.

This is a high-variance performance experiment, NOT a proven official speedup. Validate official dev answers, run the GitHub A/B benchmark, compare both outputs and then submit only if the measured improvement is reproducible.
