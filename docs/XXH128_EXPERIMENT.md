# XXH3-128 experiment — exact dedup fingerprint, one hash operation

Claude's reference parsed each event ID with xxh3_64 plus xxh3_64_with_seed for independent 64-bit/32-bit event fingerprints. This variant computes xxh3_128 once and uses the low 64 bits for the event bucket key and high 32 bits for the secondary fingerprint.

Duplicate IDs remain equal by construction. The previous approach and this approach are both probabilistic 96-bit identity fingerprints, not full byte-wise equality checks in the deduplicator. The official generator has no expected adversarial hash collisions.

Run unit tests, official 1M dev-answer SHA checks, generated datasets, and interleaved whole-process A/B benchmarks before selecting this variant. Do not infer a performance win without measurements.
