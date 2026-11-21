"""Canonicalize output parquet tables and compare to expected CSVs or known hashes.

usage:
  check.py OUT_DIR --expected DIR        compare against expected/{user_daily,campaign_daily}.csv
  check.py OUT_DIR --hash                print sorted-line sha256 of each table (dev-answer format)
  check.py OUT_A --other OUT_B           compare two output dirs
"""
import hashlib
import os
import sys

import pyarrow.parquet as pq

COLS = {
    "user_daily": ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
    "campaign_daily": ["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                       "revenue_cents", "assisted_sessions"],
}
DEV_HASH = {
    "user_daily": "5a3101f0ae53db8c7cb8b22451ba7257b6a5a73f191819e9e6ac92841820b2fc",
    "campaign_daily": "fb55ee2c4b581ebe71ddcfe5ccd2349d11ce9c2f4522ba6ae768e2952891caee",
}


def lines_from_parquet(folder, table):
    d = os.path.join(folder, table)
    files = sorted(f for f in os.listdir(d)
                   if f.endswith(".parquet") and not f.startswith((".", "_")))
    out = []
    for f in files:
        t = pq.read_table(os.path.join(d, f))
        names = set(t.column_names)
        assert names == set(COLS[table]), (table, t.column_names)
        assert str(t.schema.field("day").type) == "date32[day]", t.schema
        cols = [t.column(c).to_pylist() for c in COLS[table]]
        for row in zip(*cols):
            out.append(",".join("" if v is None else str(v) for v in row))
    return out


def lines_from_csv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rows = fh.read().split("\n")
    return [r for r in rows[1:] if r]


def sha(lines):
    return hashlib.sha256(("\n".join(sorted(lines)) + "\n").encode()).hexdigest()


def diff(a, b, label):
    sa, sb = sorted(a), sorted(b)
    if sa == sb:
        return True
    from collections import Counter
    ca, cb = Counter(a), Counter(b)
    only_a = list((ca - cb).elements())
    only_b = list((cb - ca).elements())
    print(f"  {label}: {len(only_a)} only-in-ours, {len(only_b)} only-in-expected")
    for r in sorted(only_a)[:8]:
        print("    ours    ", repr(r))
    for r in sorted(only_b)[:8]:
        print("    expected", repr(r))
    return False


def main():
    out = sys.argv[1]
    mode = sys.argv[2]
    ok = True
    for table in COLS:
        ours = lines_from_parquet(out, table)
        if mode == "--hash":
            h = sha(ours)
            match = h == DEV_HASH[table]
            ok &= match
            print(f"{table}: rows={len(ours)} sha={h[:16]} dev_match={match}")
        elif mode == "--expected":
            exp = lines_from_csv(os.path.join(sys.argv[3], table + ".csv"))
            ok &= diff(ours, exp, table)
        elif mode == "--txt":
            exp = [l.decode("utf-8", "surrogateescape") for l in open(sys.argv[3] + "." + table + ".txt", "rb").read().split(b"\n") if l]
            ok &= diff(ours, exp, table)
        elif mode == "--other":
            other = lines_from_parquet(sys.argv[3], table)
            ok &= diff(ours, other, table)
    print("OK" if ok else "MISMATCH")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
