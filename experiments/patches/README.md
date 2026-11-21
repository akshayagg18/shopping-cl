# Archived source experiments

These patches apply to the frozen baseline commit
`d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. Each materializes one later
candidate plus common profiling/valid-ingest support. Older candidates have
not been reformatted into the final code style. The common parser/allocator
helpers are behaviorally the same; formatting and diagnostic counters evolved.
This is an algorithm replay aid, not a promise of bit-identical historical ELFs.
The raw measurements identify the actual executables used in the session.

For example, create an ignored worktree with
`git worktree add --detach experiments/replay/one-page d2dc441`, apply
`one-page.patch` there, and build with the locked release profile. Read the
experiment ledger and protocol before comparing a replay. No dataset is
contained in these patches. The original and final implementations are fully
versioned; initial instrumentation-only variants have logs but no separate
archived source snapshot.
