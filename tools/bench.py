"""Repeated start-to-exit measurements with wait4 CPU/RSS and /proc RSS sampling.
Use --variants label=path ... to interleave variants and avoid ordered-run bias.
"""
import argparse, json, os, pathlib, resource, statistics, subprocess, time, threading
p=argparse.ArgumentParser();p.add_argument('--data',required=True);p.add_argument('--warmups',type=int,default=0);p.add_argument('--variants',nargs='+',required=True);p.add_argument('--runs',type=int,default=7);p.add_argument('--threads',type=int,default=2);p.add_argument('--result',required=True);p.add_argument('--rss-ms',type=float,default=50);p.add_argument('--affinity',help='comma-separated logical CPUs');p.add_argument('--profile',action='store_true');p.add_argument('--seq',action='store_true');p.add_argument('--cold',action='store_true');a=p.parse_args()
root=pathlib.Path(__file__).resolve().parents[1];data=pathlib.Path(a.data).resolve();results=[]
for run in range(-a.warmups,a.runs):
 for variant in (a.variants if run%2==0 else a.variants[::-1]):
  label,binary=variant.split('=',1);out=root/'experiments'/'bench-out'/label;out.mkdir(parents=True,exist_ok=True)
  for f in out.glob('*/*.parquet'): f.unlink()
  if a.cold:
   for f in data.rglob('*.csv'):
    with f.open('rb') as h:os.posix_fadvise(h.fileno(),0,0,os.POSIX_FADV_DONTNEED)
  env=dict(os.environ,DATA_DIR=str(data),OUTPUT_DIR=str(out),NCPU=str(a.threads),SS_TIMING='1')
  if a.profile: env['SS_PROFILE']='1'
  if a.seq: env['SS_SEQ']='1'
  log=root/'experiments'/'results'/f'{pathlib.Path(a.result).stem}-{label}-{run}.log'
  samples=[];rss_trace=[];start=time.perf_counter();next_sample=start
  with log.open('w') as h:
   affinity=set(map(int,a.affinity.split(','))) if a.affinity else None
   proc=subprocess.Popen([str(pathlib.Path(binary).resolve())],env=env,stderr=h,preexec_fn=(lambda:os.sched_setaffinity(0,affinity)) if affinity else None)
   stop=threading.Event()
   def sample_rss():
    while not stop.is_set():
     try:
      status_text=pathlib.Path(f'/proc/{proc.pid}/status').read_text()
      rss=int(status_text.split('VmRSS:')[1].split()[0])
      samples.append(rss);rss_trace.append([round((time.perf_counter()-start)*1000,3),rss])
     except (FileNotFoundError,IndexError,ProcessLookupError):pass
     stop.wait(a.rss_ms/1000)
   sampler=threading.Thread(target=sample_rss,daemon=True);sampler.start()
   pid,status,usage=os.wait4(proc.pid,0)
   elapsed=time.perf_counter()-start;proc.returncode=os.waitstatus_to_exitcode(status)
   stop.set();sampler.join()
  if proc.returncode:raise RuntimeError(f'{label} failed: {log}')
  output_bytes=sum(f.stat().st_size for f in out.glob('*/*.parquet'))
  row=dict(variant=label,run=run,wall_ms=elapsed*1000,user_ms=usage.ru_utime*1000,sys_ms=usage.ru_stime*1000,peak_rss_kib=usage.ru_maxrss,avg_rss_kib=statistics.mean(samples) if samples else 0,minor_faults=usage.ru_minflt,major_faults=usage.ru_majflt,voluntary_cs=usage.ru_nvcsw,involuntary_cs=usage.ru_nivcsw,input_blocks=usage.ru_inblock,output_blocks=usage.ru_oublock,output_bytes=output_bytes,threads=a.threads,affinity=a.affinity,rss_interval_ms=a.rss_ms,rss_trace=rss_trace,cold_hint=a.cold,log=str(log.relative_to(root)))
  row['warmup']=run<0;row['wait_method']='blocking_wait4'
  if run>=0:results.append(row)
  print(json.dumps(row),flush=True)
pathlib.Path(a.result).write_text(json.dumps(results,indent=2)+'\n')
for label in sorted({r['variant'] for r in results}):
 xs=[r['wall_ms'] for r in results if r['variant']==label];print(label,dict(median=statistics.median(xs),mean=statistics.mean(xs),stdev=statistics.stdev(xs) if len(xs)>1 else 0,min=min(xs),max=max(xs)))
