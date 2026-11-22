"""IO-only reference measurements; does not claim an absolute hardware bound."""
import pathlib,subprocess,os,json,time,statistics
r=pathlib.Path(__file__).resolve().parents[1];data=r/'experiments/data-opaque-20m'
files=sorted(data.rglob('*.csv'));result=r/'experiments/results/opaque-io-20m.json'
report={'input_bytes':sum(p.stat().st_size for p in files),'input_files':len(files),'affinity':[0,2],'cache_drop':'POSIX_FADV_DONTNEED hint; physical-read counters retained','notes':['Input C probe XOR-reads every byte using AVX2 and MAP_POPULATE.','Writes use actual preloaded Parquet bytes into a regular file on the output filesystem, with no fsync, matching the pipeline durability contract.','These are observed IO-only reference costs on this VM, not proven physical minima or official-hardware predictions.'],'read_warm':[],'read_cold_hint':[],'writes':[]}
def save():result.write_text(json.dumps(report,indent=2)+'\n')
for cold in [False,True]:
 for i in range(7 if not cold else 3):
  if cold:
   for p in files:
    with p.open('rb') as f:os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
  p=subprocess.Popen([r/'experiments/bin/bandwidth',*files],stdout=subprocess.PIPE,preexec_fn=lambda:os.sched_setaffinity(0,{0,2}))
  _,status,usage=os.wait4(p.pid,0);p.returncode=os.waitstatus_to_exitcode(status)
  assert not p.returncode
  row=json.loads(p.stdout.read());row.update(gb_s=report['input_bytes']/row['seconds']/1e9,cpu_s=usage.ru_utime+usage.ru_stime,major_faults=usage.ru_majflt,input_blocks=usage.ru_inblock)
  report['read_cold_hint' if cold else 'read_warm'].append(row);save()
paths=list((r/'experiments/reference-current-20m').glob('*/*.parquet'));payload=b''.join(p.read_bytes() for p in paths);report['output_bytes']=len(payload);report['output_files']=len(paths)
p=r/'experiments/io-only-output.tmp'
try:
 for i in range(7):
  start=time.perf_counter();fd=os.open(p,os.O_CREAT|os.O_TRUNC|os.O_WRONLY,0o600);view=memoryview(payload)
  while view:view=view[os.write(fd,view):]
  os.close(fd);elapsed=time.perf_counter()-start
  report['writes'].append({'seconds':elapsed,'gb_s':len(payload)/elapsed/1e9});save()
finally:p.unlink(missing_ok=True)
report['reference_ms']={'read_warm':statistics.median(x['seconds'] for x in report['read_warm'])*1000,'read_cold_hint':statistics.median(x['seconds'] for x in report['read_cold_hint'])*1000,'write':statistics.median(x['seconds'] for x in report['writes'])*1000};save();print(json.dumps(report['reference_ms']))
