"""Measure Docker state start-to-exit separately from CLI overhead; fresh containers."""
import argparse,datetime,json,os,pathlib,statistics,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--data',required=True);p.add_argument('--image',required=True);p.add_argument('--runs',type=int,default=5);p.add_argument('--result',required=True);p.add_argument('--threads',type=int,default=2);a=p.parse_args()
r=pathlib.Path(__file__).resolve().parents[1];data=pathlib.Path(a.data).resolve();out=r/'experiments/docker-out';spill=r/'experiments/docker-scratch'
for d in [out,spill]:d.mkdir(exist_ok=True);d.chmod(0o777)
rows=[]
def docker(*args):return subprocess.check_output(['docker',*args],text=True)
def stamp(s):return datetime.datetime.fromisoformat(s.rstrip('Z')[:26]+'+00:00')
for k in range(a.runs):
 for f in out.glob('*/*.parquet'):f.unlink()
 cid=docker('create','--network','none','--read-only','--user','1000:1000','--cpuset-cpus','0,2','--tmpfs','/tmp:rw,size=64m','-e','DATA_DIR=/data','-e','OUTPUT_DIR=/output','-e','SCRATCH_DIR=/scratch','-e','HOME=/scratch','-e',f'NCPU={a.threads}','-e','MEM_GB=24','-e','SS_TIMING=1','-v',str(data)+':/data:ro','-v',str(out)+':/output','-v',str(spill)+':/scratch',a.image).strip()
 t=time.perf_counter();p=subprocess.run(['docker','start','-a',cid],text=True,capture_output=True);cli_ms=(time.perf_counter()-t)*1000
 state=json.loads(docker('inspect',cid))[0]['State'];docker('rm',cid)
 if state['ExitCode']:raise RuntimeError(state,p.stderr)
 (r/f'experiments/results/docker-{k}.log').write_text(p.stderr)
 row=dict(run=k,container_ms=(stamp(state['FinishedAt'])-stamp(state['StartedAt'])).total_seconds()*1000,cli_ms=cli_ms,exit_code=state['ExitCode'],output_bytes=sum(f.stat().st_size for f in out.glob('*/*.parquet')));rows.append(row);print(json.dumps(row),flush=True)
pathlib.Path(a.result).write_text(json.dumps(rows,indent=2)+'\n')
x=[v['container_ms'] for v in rows];print(dict(median=statistics.median(x),mean=statistics.mean(x),sd=statistics.stdev(x) if len(x)>1 else 0,min=min(x),max=max(x)))
