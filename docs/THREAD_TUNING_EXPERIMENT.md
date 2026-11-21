# Four-physical-core tuning experiment

No algorithm change: the only difference from the 1.73-second original Claude submission is limiting Rayon thread count to four rather than eight. The official container offers 8 logical CPUs across 4 physical cores with hyperthreading. A memory-bandwidth- and page-fault-sensitive pipeline may benefit from fewer active threads.

The A/B CI benchmark uses the same 20-million-event generated corpus and requests NCPU=8 for both versions; the baseline uses eight workers while this candidate caps at four. It reports CPU topology and full-process runtimes, then checks the official 1M development answer and exact Parquet equivalence.

GitHub-hosted runner topology may differ from the official scoring machine. Only submit if performance evidence is convincing.
