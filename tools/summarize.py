"""Summarize barrier deltas and opt-in spans, without adding concurrent CPU time."""
import collections,json,pathlib,re,statistics,sys
root=pathlib.Path(__file__).resolve().parents[1]
for name in sys.argv[1:]:
 print('\nRESULT',name)
 runs=json.loads(pathlib.Path(name).read_text());by=collections.defaultdict(list);stages=collections.defaultdict(lambda:collections.defaultdict(list));spans=collections.defaultdict(list)
 for run in runs:
  by[run['variant']].append(run);prev=0
  for line in (root/run['log']).read_text().splitlines():
   m=re.match(r'\s*([\d.]+) ms  (.*)',line)
   if m:
    t=float(m[1]);stages[run['variant']][m[2]].append(t-prev);prev=t
   if line.startswith('PROFILE '):
    d=dict(x.split('=',1) for x in line.split()[1:]);spans[d['stage']].append(d)
 for variant,rs in by.items():
  print(variant)
  for field in ['wall_ms','user_ms','sys_ms','peak_rss_kib','avg_rss_kib','minor_faults','major_faults','output_bytes']:
   vs=[r[field] for r in rs];print(field,dict(median=round(statistics.median(vs),3),mean=round(statistics.mean(vs),3),sd=round(statistics.stdev(vs),3) if len(vs)>1 else 0,min=round(min(vs),3),max=round(max(vs),3)))
  for stage,vs in stages[variant].items():print('barrier_delta',stage,round(statistics.median(vs),3))
 for stage,ss in spans.items():
  walls=[float(s['wall_ms']) for s in ss];cpus=[float(s['thread_ms']) for s in ss]
  print('span',stage,'tasks',len(ss),'sum_thread_ms',round(sum(cpus),3),'wall_median',round(statistics.median(walls),3),'wall_max',max(walls),'max_records',max(int(s['records']) for s in ss))
