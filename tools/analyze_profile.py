"""Summarize diagnostic stage envelopes without adding overlapping task spans."""
import argparse,json,pathlib,re,statistics,collections
p=argparse.ArgumentParser();p.add_argument('measurements');p.add_argument('--variant',required=True);p.add_argument('--output',required=True);a=p.parse_args()
root=pathlib.Path(__file__).resolve().parents[1]
rows=json.loads(pathlib.Path(a.measurements).read_text());rows=[r for r in rows if r['variant']==a.variant]
phases=['open','links_phase','parse_phase','dedup_phase','session_phase','output','release']
records=collections.defaultdict(list);tasks=collections.defaultdict(list);alloc=[]
for row in rows:
 text=(root/row['log']).read_text()
 for line in text.splitlines():
  if line.startswith('PROFILE_SUMMARY'):
   alloc.append({k:int(v) for k,v in re.findall(r'(\w+)=(\d+)',line)});continue
  if not line.startswith('PROFILE '):continue
  d=dict(x.split('=',1) for x in line.split()[1:]);stage=d.pop('stage');d={k:float(v) for k,v in d.items()}
  if stage not in phases:tasks[stage].append(d);continue
  end=d['end_ms'];start=end-d['wall_ms']
  # Process launch/thread-pool startup and 50ms sampling introduce a small offset.
  samples=[v for t,v in row['rss_trace'] if start<=t<=end]
  d['avg_sampled_rss_kib']=statistics.mean(samples) if samples else None
  d['peak_sampled_rss_kib']=max(samples) if samples else None
  if stage=='output':d['bytes']=row['output_bytes']
  records[stage].append(d)
def median(v):
 xs=[x for x in v if x is not None];return statistics.median(xs) if xs else None
report={'source':a.measurements,'variant':a.variant,'n':len(rows),'notes':['Phase process CPU is summed across threads. Nested task spans overlap and are not added to phase walls.','RSS is sampled, not exact per-stage peak, and has a small process-start offset.','Byte counts after parsing describe logical representations, not hardware memory traffic.','Session records count incoming candidates plus clicks, including duplicate losers before live gather.'],'stages':{},'tasks':{},'allocations':{}}
for stage in phases:
 ds=records[stage]
 if ds:report['stages'][stage]={k:median(d[k] for d in ds) for k in ds[0] if k not in ['id','worker','end_ms']}
for stage,ds in tasks.items():
 workers=collections.defaultdict(float)
 for d in ds:workers[int(d['worker'])]+=d['thread_ms']
 report['tasks'][stage]={'n':len(ds),'median_wall_ms':median(d['wall_ms'] for d in ds),'max_wall_ms':max(d['wall_ms'] for d in ds),'max_records':max(d['records'] for d in ds),'summed_thread_cpu_ms':sum(d['thread_ms'] for d in ds),'worker_thread_cpu_ms':dict(workers)}
if alloc:report['allocations']={k:median(d[k] for d in alloc) for k in alloc[0]}
pathlib.Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
