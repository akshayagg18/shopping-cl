"""Mutate a small generated dataset with dirt the generator never writes."""
import os
import random
import shutil
import sys

src, dst, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
rng = random.Random(seed)
shutil.rmtree(dst, ignore_errors=True)
shutil.copytree(src, dst)

TS_MUT = [
    lambda t: t + "\r", lambda t: t.replace("T", "t"), lambda t: "0000" + t if t.isdigit() else t,
    lambda t: t[:-1] + "+00:00" if t.endswith("Z") else t, lambda t: t[:-1] + "-00:00" if t.endswith("Z") else t,
    lambda t: t.replace(":", "", 1), lambda t: "9" * 25, lambda t: " " + t, lambda t: t + " ",
    lambda t: t[:17] + "60Z" if len(t) == 20 else t, lambda t: t.replace("-", "‐", 1),
]
VAL_MUT = [
    lambda v: " " + v, lambda v: v + " ", lambda v: v.upper(), lambda v: '"' + v + '"', lambda v: "",
    lambda v: v + "\r", lambda v: '"' + v, lambda v: v + '"', lambda v: 'x"' + v + '"',
    lambda v: '""', lambda v: v + "é",
]


def mutate_line(line, ts_cols, folder="events"):
    fields = line.split(",")
    global rng
    saved = rng
    if folder == "clicks":
        # all copies of a click must stay identical: derive the mutation from the click_id
        rng = random.Random(fields[0].strip('"') + str(seed))
    try:
        return _mutate(fields, ts_cols, folder, line)
    finally:
        rng = saved


def _mutate(fields, ts_cols, folder, line):
    r = rng.random()
    if r < 0.02:
        return ""
    if r < 0.04:
        return line + ",extra"
    i = rng.randrange(len(fields))
    if folder == "events" and i == 3:
        fields[3] = '"' + fields[3].strip('"') + '"'  # ingest_ts stays valid F1
        return ",".join(fields)
    if i in ts_cols and rng.random() < 0.6:
        fields[i] = rng.choice(TS_MUT)(fields[i].strip('"'))
    else:
        fields[i] = rng.choice(VAL_MUT)(fields[i].strip('"'))
    return ",".join(fields)


for folder, ts_cols in (("events", {2, 3}), ("clicks", {3}), ("links", set())):
    d = os.path.join(dst, folder)
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        with open(p, encoding="utf-8", newline="") as fh:
            text = fh.read()
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        out = [lines[0]]
        for line in lines[1:]:
            fid = line.split(",")[0].strip('"')
            hit = (random.Random(fid + str(seed)).random() < 0.03) if folder == "clicks" else (rng.random() < 0.03)
            if hit:
                out.append(mutate_line(line, ts_cols if folder != "events" else {2}, folder))
            else:
                out.append(line)
            if rng.random() < 0.002:
                out.append(line)  # exact duplicate line
        body = "\n".join(out)
        if rng.random() < 0.5:
            body += "\n"
        if rng.random() < 0.1 and len(out) > 1:
            body += "\r\n"
        with open(p, "w", encoding="utf-8", newline="") as fh:
            fh.write(body)
# a CRLF file and an empty (0-byte) file
open(os.path.join(dst, "events", "zz-empty.csv"), "w").close()
with open(os.path.join(dst, "clicks", "zz-crlf.csv"), "w", newline="") as fh:
    fh.write("click_id,user_id,campaign_id,ts\r\nkx1,uabc,c_1,1\r\n")
open(os.path.join(dst, "links", "notcsv.txt"), "w").write("a,b\n")
