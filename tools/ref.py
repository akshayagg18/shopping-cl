"""Slow, literal reference implementation of rules.md. Writes canonical CSV lines.
usage: ref.py DATA_DIR OUT_PREFIX   -> OUT_PREFIX.user_daily.txt / .campaign_daily.txt
"""
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timezone

DAY = 86_400_000
data, prefix = sys.argv[1], sys.argv[2]


def read_lines(path):
    with open(path, "rb") as fh:
        raw = fh.read()
    parts = raw.split(b"\n")
    if parts and parts[-1] == b"":
        parts.pop()
    return parts[1:]  # header


def split(line, n):
    if line == b"":
        return None
    fs = line.split(b",")
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


F12 = re.compile(rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(Z|[+-]\d{2}:\d{2})$")


def ts(b):
    if b == b"":
        return None
    if all(48 <= c <= 57 for c in b):
        v = int(b)
        return v
    m = F12.match(b)
    if not m or b.endswith(b"\n"):
        return None
    if not all(48 <= c <= 57 or c in b"-T:Z+" for c in b):
        return None
    y, mo, d, h, mi, s = (int(x) for x in m.groups()[:6])
    if h > 23 or mi > 59 or s > 59:
        return None
    try:
        dt = datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
    except ValueError:
        return None
    ms = int((dt - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()) * 1000
    z = m.group(7)
    if z != b"Z":
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000
        ms = ms - off if z[0:1] == b"+" else ms + off
    return ms


man = json.load(open(os.path.join(data, "manifest.json")))
ws, we = ts(man["window_start"].encode()), ts(man["window_end"].encode())

cats = {}
for l in read_lines(os.path.join(data, "products.csv")):
    f = split(l, 2)
    cats[f[0]] = f[1]
chan = {}
for l in read_lines(os.path.join(data, "campaigns.csv")):
    f = split(l, 2)
    chan[f[0]] = f[1]

parent = {}


def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for p in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
    for l in read_lines(p):
        f = split(l, 2)
        if f is None or f[0] == b"" or f[1] == b"":
            continue
        a, b = find(f[0]), find(f[1])
        if a != b:
            parent[a] = b
name = {}
for x in list(parent):
    r = find(x)
    if r not in name or x < name[r]:
        name[r] = x


def person(u):
    return name[find(u)] if u in parent else u


events = {}
for p in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
    for l in read_lines(p):
        f = split(l, 7)
        if f is None or f[0] == b"":
            continue
        ing = ts(f[3])
        if f[0] not in events or ing < events[f[0]][0]:
            events[f[0]] = (ing, f)
by_person = defaultdict(list)
for eid, (ing, f) in events.items():
    t = ts(f[2])
    if f[1] == b"" or t is None or not (ws <= t < we) or f[4] not in (b"view", b"cart", b"purchase") or f[5] == b"":
        continue
    price = 0
    if f[4] == b"purchase":
        if f[6] == b"" or not all(48 <= c <= 57 for c in f[6]):
            continue
        price = int(f[6])
        if not (1 <= price <= 10_000_000):
            continue
    if ing - t > DAY:
        continue
    by_person[person(f[1])].append((t, f[4] == b"purchase", price, cats.get(f[5], b"unknown")))

clicks = defaultdict(list)
seen = set()
for p in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
    for l in read_lines(p):
        f = split(l, 4)
        if f is None or f[0] == b"" or f[0] in seen:
            continue
        seen.add(f[0])
        t = ts(f[3])
        if f[1] == b"" or t is None or not (ws - 7 * DAY <= t < we) or f[2] not in chan:
            continue
        clicks[person(f[1])].append((t, f[0], f[2]))

ud = {}
cd = defaultdict(lambda: [0, 0, 0, 0])
for per, evs in by_person.items():
    evs.sort(key=lambda e: e[0])
    sessions = []
    for e in evs:
        if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
            sessions[-1].append(e)
        else:
            sessions.append([e])
    cks = clicks.get(per, [])
    for s in sessions:
        start = s[0][0]
        day = date.fromtimestamp(start // 1000 // 86400 * 86400, tz=timezone.utc) if False else \
            datetime.fromtimestamp((start // DAY) * 86400, tz=timezone.utc).date()
        q = [c for c in cks if c[0] <= start and start - c[0] <= 7 * DAY]
        if q:
            best = sorted(q, key=lambda c: (-c[0], c[1]))[0]
            credit = best[2]
        else:
            credit = b"organic"
        rev = sum(e[2] for e in s if e[1])
        purch = any(e[1] for e in s)
        k = (credit, day)
        cd[k][0] += 1
        cd[k][1] += purch
        cd[k][2] += rev
        for c in {c[2] for c in q}:
            cd[(c, day)][3] += 1
        row = ud.setdefault((per, day), [0, 0, 0, defaultdict(int)])
        row[0] += 1
        row[1] += len(s)
        row[2] += rev
        for e in s:
            if e[1]:
                row[3][e[3]] += e[2]

with open(prefix + ".user_daily.txt", "wb") as fh:
    for (per, day), r in ud.items():
        top = b""
        if r[3]:
            top = sorted(r[3].items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        fh.write(b",".join([per, str(day).encode(), str(r[0]).encode(), str(r[1]).encode(), str(r[2]).encode(), top]) + b"\n")
with open(prefix + ".campaign_daily.txt", "wb") as fh:
    for (c, day), r in cd.items():
        ch = b"organic" if c == b"organic" else chan[c]
        fh.write(b",".join([c, ch, str(day).encode()] + [str(x).encode() for x in r]) + b"\n")
