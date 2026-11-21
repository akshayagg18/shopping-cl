# Validation

Downloaded official assets are ignored, not vendored. `tools/validate.py BINARY`
runs all 36 official fixtures, official seed-1 1M expected CSV comparison,
20K generated literal reference, and four supplied fuzz mutations compared to
`tools/ref.py`. Set POLARS_MAX_THREADS=2 for this VM. All checks use fresh output
folders: Parquet file counts can vary between executions.

The pinned README mentions an independent Polars implementation, but it is not
in the repository; the downloaded official Polars starter is an empty skeleton.
We can reproduce the supplied literal Python reference and official fixtures,
not that undocumented earlier Polars test. No claim that it was rerun.
