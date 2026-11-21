"""Interleaved image comparison using Docker state timestamps, not CLI latency."""
import argparse,datetime,json,pathlib,statistics,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--data',required=True);p.add_argument('--variants',nargs='+',required=True);p.add_argument('--runs',type=int,default=5);p.add_argument('--warmups',type=int,default=1);p.add_argument('--result',required=True);a=p.parse_args()
r=pathlib.Path(__file__).resolve().parents[1];data=pathlib.Path(a.data).resolve();rows=[]
def docker(*args):return subprocess.check_output(['docker',*args],text=True)
def stamp(s):return datetime.datetime.fromisoformat(s.rstrip('Z')[:26]+'+00:00')
for k in range(-a.warmups,a.runs):
 for v in (a.variants if k%2==0 else a.variants[::-1]):
  label,image=v.split('=',1);out=r/'experiments/docker-out'/label;out.mkdir(parents=True,exist_ok=True);out.chmod(0o777)
  for name in ['user_daily','campaign_daily']:(out/name).mkdir(exist_ok=True);(out/name).chmod(0o777)
  for f in out.glob('*/*.parquet'):f.unlink()
  spill=r/'experiments/docker-scratch';spill.mkdir(exist_ok=True);spill.chmod(0o777)
  cid=docker('create','--network','none','--read-only','--user','1000:1000','--cpuset-cpus','0,2','--tmpfs','/tmp:rw,size=64m','-e','DATA_DIR=/data','-e','OUTPUT_DIR=/output','-e','SCRATCH_DIR=/scratch','-e','HOME=/scratch','-e','NCPU=8','-e','MEM_GB=24','-e','SS_TIMING=1','-v',str(data)+':/data:ro','-v',str(out)+':/output','-v',str(spill)+':/scratch',image).strip()
  start=time.perf_counter();proc=subprocess.run(['docker','start','-a',cid],text=True,capture_output=True);cli_ms=(time.perf_counter()-start)*1000
  inspect=json.loads(docker('inspect',cid))[0];state=inspect['State'];docker('rm',cid)
  (r/f'experiments/results/docker-large-{label}-{k}.log').write_text(proc.stderr)
  if proc.returncode or state['ExitCode'] or state['Status']!='exited':raise RuntimeError(state,proc.stderr)
  row=dict(variant=label,run=k,image=image,container_ms=(stamp(state['FinishedAt'])-stamp(state['StartedAt'])).total_seconds()*1000,cli_ms=cli_ms,exit_code=0,output_bytes=sum(f.stat().st_size for f in out.glob('*/*.parquet')),warmup=k<0,requested_affinity='0,2',observed_affinity='0,1,2 (separate static probe; rootless cpuset ignored)')
  if k>=0:rows.append(row)
  print(json.dumps(row),flush=True)
pathlib.Path(a.result).write_text(json.dumps(rows,indent=2)+'\n')
for v in sorted({d['variant'] for d in rows}):
 x=[d['container_ms'] for d in rows if d['variant']==v];print(v,dict(median=statistics.median(x),mean=statistics.mean(x),sd=statistics.stdev(x),min=min(x),max=max(x)))
