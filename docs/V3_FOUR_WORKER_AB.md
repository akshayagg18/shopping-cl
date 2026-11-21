# Claude V3 + 4-worker tuning experiment

Original pinned 1.73-second user reference: `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`.
Claude's subsequent V3 on main: `bc272b815059c02dd95edfeb0217b34c65667d34`.

This branch differs from Claude V3 in one algorithm-neutral parameter: Rayon worker thread cap `min(NCPU,4)`. The official runner has 4 physical cores with 2 hyperthreads per core; a different, original-Claude-based 20M GitHub Actions A/B demonstrated 2.566 s at 8 workers vs 2.273 s at 4 workers (11.4% less wall time) with both official 1M dev answers matching exactly.

This branch's CI compares the newer Claude V3 code against this single thread-cap modification on the same 20M generated dataset and validates Parquet outputs. The benchmark is not an official score; only deploy if measured results are repeatable. All other V3 enhancements, including SWAR timestamp parsing, page mapping and in-place dedup, are preserved.
