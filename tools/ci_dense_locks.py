"""Same-source 60M diagnosis; retain stage logs and resource counters in a check."""
import datetime,json,os,pathlib,re,shutil,statistics,subprocess,sys,time,urllib.request,zipfile
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r);e=r/'experiments';(e/'results').mkdir(parents=True,exist_ok=True);(e/'bin').mkdir(exist_ok=True)
target=os.environ['CANDIDATE_SHA'];prior='fef07368caf2e161fbf91dbf67e8176574fcfaa9';repo=os.environ.get('GITHUB_REPOSITORY','akshayagg18/shopping-cl');result=e/'results/dense-lock-profile.json'
if '--publish' in sys.argv:
 report=json.loads(result.read_text()) if result.exists() else {'success':False,'error':'No result'}
 summary='Same-artifact regression diagnosis; **not an official score**.\n\n```json\n'+json.dumps(report,indent=2)+'\n```';assert len(summary)<60000,len(summary)
 payload={'name':'Codex dense lock profile','head_sha':target,'status':'completed','conclusion':'success' if report.get('success') else 'failure','output':{'title':'60M lock counts and model timings','summary':summary}}
 p=e/'check-diagnosis.json';p.write_text(json.dumps(payload));subprocess.run(['gh','api','--method','POST','repos/'+repo+'/check-runs','--input',str(p)],check=True);raise SystemExit()
report={'success':False,'source':target,'prior':prior,'official_score':False,'run_url':'https://github.com/'+repo+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID',''),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'studies':{}}
def save():result.write_text(json.dumps(report,indent=2)+'\n')
def run(args,**kw):
 args=list(map(str,args));p=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,**kw);print(p.stdout,flush=True)
 if p.returncode:report['last_failed_output']=p.stdout[-8000:];save();raise subprocess.CalledProcessError(p.returncode,args)
 return p.stdout
try:
 cpus=sorted(os.sched_getaffinity(0));threads=len(cpus)
 report['hardware']={'affinity':cpus,'topology':run(['lscpu','-e=CPU,CORE,SOCKET,NODE,CACHE']),'cpu_model':next(x.split(':',1)[1].strip() for x in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')),'caches':{str(p):p.read_text().strip() for p in pathlib.Path('/sys/devices/system/cpu/cpu0/cache').glob('index*/size')}};save()
 for label,commit in [('prior',prior),('candidate',target)]:
  context=e/('context-'+label);context.mkdir(exist_ok=True)
  archive=e/(label+'.tar')
  with archive.open('wb') as f:subprocess.run(['git','archive',commit],stdout=f,check=True)
  run(['tar','-xf',archive,'-C',context]);archive.unlink()
  if label=='candidate':
   p=context/'Dockerfile';text=p.read_text();assert 'cargo build --release --locked' in text;p.write_text(text.replace('cargo build --release --locked','cargo build --release --locked --features diagnostics'));report['candidate_build_features']='diagnostics; SS_PROFILE remains unset'
  start=time.monotonic();run(['docker','build','--platform','linux/amd64','-t','shop-diag-'+label,context]);report[label+'_build_s']=time.monotonic()-start
  cid=run(['docker','create','shop-diag-'+label]).strip();run(['docker','cp',cid+':/ssp',e/'bin'/('diag-'+label)]);run(['docker','rm',cid])
 run(['docker','builder','prune','--all','--force']);save()
 assets=e/'assets';assets.mkdir(exist_ok=True);z=assets/'generator.zip';urllib.request.urlretrieve('https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip',z)
 with zipfile.ZipFile(z) as f:f.extractall(assets)
 run(['cargo','build','--release','--locked','--manifest-path',assets/'generator/Cargo.toml']);gen=assets/'generator/target/release/generate'
 for seed in [20261123]:
  data=e/('data-'+str(seed));run([gen,'--seed',seed,'--events',60_000_000,'--clicks',15_000_000,'--threads',threads,'--output',data])
  variants={'prior':{},'old':{'SS_LOCK_DIAG':'1'},'stratified':{'SS_LOCK_DIAG':'1','SS_STRATIFY_USER':'1'},'ordered':{'SS_LOCK_DIAG':'1','SS_USER_FIRST':'1'},'both':{'SS_LOCK_DIAG':'1','SS_STRATIFY_USER':'1','SS_USER_FIRST':'1'},'readonly':{'SS_LOCK_DIAG':'1','SS_READONLY_USER':'1'},'combined':{'SS_LOCK_DIAG':'1','SS_STRATIFY_USER':'1','SS_USER_FIRST':'1','SS_READONLY_USER':'1'}}
  settings={};args=[]
  for label,env in variants.items():
   args.append(label+'='+str(e/'bin'/('diag-prior' if label=='prior' else 'diag-candidate')));settings[label]=env
  path=e/'results'/('diag-'+str(seed)+'.json')
  run(['python','tools/bench.py','--data',data,'--warmups','1','--runs',3,'--threads',threads,'--affinity',','.join(map(str,cpus)),'--variants',*args,'--variant-env',json.dumps(settings),'--result',path])
  rows=json.loads(path.read_text());study={'manifest':json.loads((data/'manifest.json').read_text()),'variants':{}};report['studies'][str(seed)]=study;save()
  for label in variants:
   rs=[x for x in rows if x['variant']==label];xs=[x['wall_ms'] for x in rs];stages=[];logs=[]
   for row in rs:
    log=(r/row['log']).read_text();last=0;times={}
    for ms,name in re.findall(r'^\s*([0-9.]+) ms\s+(\S+)\s*$',log,re.M):
     at=float(ms);times[name]=at-last;last=at
    stages.append(times);logs.append(log[-3500:])
   record={'n':len(xs),'median_ms':statistics.median(xs),'mean_ms':statistics.mean(xs),'sd_ms':statistics.stdev(xs),'min_ms':min(xs),'max_ms':max(xs),'samples_ms':xs,'settings':settings[label]}
   for key in ['user_ms','sys_ms','peak_rss_kib','avg_rss_kib','minor_faults','major_faults','voluntary_cs','involuntary_cs']:record[key]=statistics.median(x[key] for x in rs)
   record['stage_medians_ms']={key:statistics.median(x[key] for x in stages if key in x) for key in {k for stage in stages for k in stage}}
   record['individual_stage_logs']=logs
   study['variants'][label]=record;save()
   run(['python','tools/check.py',e/'bench-out'/label,'--other',e/'bench-out/prior']);record['exact_output']='passed';save()
  try:
   if not shutil.which('strace'):run(['sudo','apt-get','install','-y','strace'])
   trace=e/'results/strace-locks.txt';env=dict(os.environ,DATA_DIR=str(data),OUTPUT_DIR=str(e/'strace-output'),NCPU=str(threads),SS_TIMING='1',SS_LOCK_DIAG='1')
   run(['strace','-f','-c','-e','trace=futex,mmap,munmap,madvise','-o',trace,e/'bin/diag-candidate'],env=env)
   report['strace_note']='Untimed diagnostic with ptrace overhead; counts identify syscall types, not native elapsed cost.';report['strace_summary']=trace.read_text();save()
  except Exception as error:report['strace_error']=str(error)[:1000];save()
  report['studies'][str(seed)]=study;save();shutil.rmtree(data)
  for label in variants:shutil.rmtree(e/'bench-out'/label)
 report['success']=True
except Exception as error:report['error']=str(error)[:1000];raise
finally:save()
