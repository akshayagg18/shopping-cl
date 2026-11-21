# Equal timestamp click tie-sort micro-optimization

Claude V3 + Rayon cap at 4 is the A/B baseline. For >=64 clicks in a customer group, sort by timestamp only rather than repeatedly comparing click ID strings in the sort comparator. Scan each equal-timestamp run and swap the byte-wise minimum ID to its first position. Credit picks that first element of the newest eligible timestamp run, and assistance is unaffected because it uses a set of campaigns across the window. For small groups retain original sort. All published timestamp and ID rules are unchanged.

This is an isolated, speculative optimization; it is not known to be faster. GitHub Actions runs exact 1M dev-answer comparisons and repeated 20M A/B on the same machine. Do not submit unless measured faster than Claude V3+4 workers.
