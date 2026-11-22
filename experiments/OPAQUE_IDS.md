# Opaque-ID optimization experiment

The live generator changed during the task. The old generator SHA256 was
`13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e`;
the current pinned generator SHA256 is
`78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a`.
Current rules explicitly require opaque strings: one in eight IDs has a
32-character hexadecimal suffix. No generator inversion or inferred identity
links are used by this candidate. Only input link edges connect people.

The previous fully exact Rust source fef07368 used global byte dictionaries for
uncommon strings. On current 60M data, instrumentation recorded 10,822,794 user
interner calls and 7,824,204 event dictionary calls. An untimed strace profile
attributed 98.65% of traced syscall time to futex. This identifies contention;
strace time is not native pipeline wall time. The old dense-ID experiment never
activated on this workload. Its old-generator wins are retired.

## Controlled experiments

Local Xeon8573C VM, 2-CPU quota, 8GiB, affinity0,2, 4 workers. Seven interleaved
runs, one warmup each, current-generator 5M seed20261123 / 1.25M clicks.
Each row compares within its own paired batch; do not multiply these speedups.
Raw measurements are retained locally; the hosted receipt will independently
rebuild and record the exact source/compiler/artifact/asset hashes.

|Experiment|Control median ms|New median ms|Reduction|Correctness|Decision|
|---|---:|---:|---:|---|---|
|Event dictionary shards 1 to64|1122.07|1047.61|6.6%|Both tables exact against D2|Keep for fallback|
|Worker-local exact user dictionary cache|1098.00|1038.85|5.4%|Units, fixtures, references, fuzz, adversarial pass|Validate large|
|Event field references vs same binary owned mode|1047.98|793.15|24.3%|Both tables exact against D2-derived control|Validate large|

The frozen D2 source measured1008.15ms and the prior exact source1251.07ms
in the separate first batch. That prior is not competitive on current long IDs.
No official score or sub-0.4-second result has been established.

## Representation and equality

The event copy remains16 bytes. Short event IDs are encoded reversibly when
possible. Other fields of length1..63 use an exact address offset35 + length6
inside the existing48-bit token. This is generic source addressing, with no
interpretation of an ID's spelling. Input address span is checked against32GiB.
Longer fields, unsuitable mapping spans, and non-source strings use the exact
owned fallback. Reserved tokens retain the full signed uncommon ingestion
seconds and extended event locations. Dedup still occurs before validation.

For references, a full-byte hash selects a partition/probe fingerprint. An equal
fingerprint is followed by full-byte equality. Equal hashes never establish ID
equality. Source mappings remain alive through dedup; reconstructed pointers
use exposed provenance and the original field bounds. No references are read
after unmapping. The existing48-bit representation is unchanged in size.

User-ID fallback keys remain injective across all shards. Worker-local maps
cache the global exact dictionary result using full-byte keys, so another
thread always obtains the same identity. No connected components are inferred.

## Validation and controls

The feature-enabled source passed22 release unit tests, all36 official fixtures,
old and current1M development answers, literal independent reference plus four
fuzz mutations, and adversarial long UTF-8/signed-ingestion/boundary cases.
A targeted test forces all event hashes equal, including a compact ID and
multiple distinct source fields, and verifies distinct IDs survive while an
earlier invalid copy still suppresses later valid copies. Tests also exercise
concurrent sharded insertion, exact reverse decoding, repeated fields at
separate locations, owned/ref equality, and0/1/63/64/65537-byte fields.

Default candidate settings enable these changes. Experimental controls:
`SS_EVENT_REFS=0`, `SS_ID_LOCAL=0`, `SS_ID_SHARDS=1`, `SS_EVENT_SHARDS=1`.
Normal release diagnostics remain compiled out. Current-generator20M, two60M
seeds and fresh Docker validation subsequently passed for source ae0cf75; see
FIRST_SUBMISSION_REPORT.md and the corrected run44995086022 for all distributions.
The official score and requested sub-0.4 target remain unproven.
