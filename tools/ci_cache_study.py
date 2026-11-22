"""Same-source 60M diagnosis; retain stage logs and resource counters in a check."""
import datetime,json,os,pathlib,re,shutil,statistics,subprocess,sys,time,urllib.request,zipfile
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r);e=r/'experiments';(e/'results').mkdir(parents=True,exist_ok=True);(e/'bin').mkdir(exist_ok=True)
target=os.environ['CANDIDATE_SHA'];prior='fef07368caf2e161fbf91dbf67e8176574fcfaa9';repo=os.environ.get('GITHUB_REPOSITORY','akshayagg18/shopping-cl');result=e/'results/cache-study.json'
if '--publish' in sys.argv:
 report=json.loads(result.read_text()) if result.exists() else {'success':False,'error':'No result'}
 summary='Same-artifact regression diagnosis; **not an official score**.\n\n```json\n'+json.dumps(report,indent=2)+'\n```';assert len(summary)<60000,len(summary)
 payload={'name':'Codex cache and worker study','head_sha':target,'status':'completed','conclusion':'success' if report.get('success') else 'failure','output':{'title':'Current-generator cache and worker matrix','summary':summary}}
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
 for label,commit in [('candidate',target)]:
  context=e/('context-'+label);context.mkdir(exist_ok=True)
  archive=e/(label+'.tar')
  with archive.open('wb') as f:subprocess.run(['git','archive',commit],stdout=f,check=True)
  run(['tar','-xf',archive,'-C',context]);archive.unlink()
  start=time.monotonic();run(['docker','build','--platform','linux/amd64','-t','shop-diag-'+label,context]);report[label+'_build_s']=time.monotonic()-start
  cid=run(['docker','create','shop-diag-'+label]).strip();run(['docker','cp',cid+':/ssp',e/'bin'/('diag-'+label)]);run(['docker','rm',cid])
 run(['docker','builder','prune','--all','--force']);save()
 assets=e/'assets';assets.mkdir(exist_ok=True);z=assets/'generator.zip';urllib.request.urlretrieve('https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip',z)
 report['generator_sha256']=__import__('hashlib').sha256(z.read_bytes()).hexdigest();assert report['generator_sha256']=='78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a'
 with zipfile.ZipFile(z) as f:f.extractall(assets)
 run(['cargo','build','--release','--locked','--manifest-path',assets/'generator/Cargo.toml']);gen=assets/'generator/target/release/generate'
 for seed,events,repeats in [(20261123,20_000_000,7),(20261123,60_000_000,3),(20261124,60_000_000,3)]:
  data=e/('data-'+str(seed)+'-'+str(events));run([gen,'--seed',seed,'--events',events,'--clicks',events//4,'--threads',threads,'--output',data])
  variants={'auto':{},'p256d1024':{'SS_P2B':'8','SS_PDB':'10'},'p512d1024':{'SS_P2B':'9','SS_PDB':'10'},'p1024d1024':{'SS_P2B':'10','SS_PDB':'10'},'p512d512':{'SS_P2B':'9','SS_PDB':'9'},'p1024d512':{'SS_P2B':'10','SS_PDB':'9'},'p512d2048':{'SS_P2B':'9','SS_PDB':'11'},'two_workers':{'SS_P2B':'9','SS_PDB':'10','NCPU':'2'},'eight_workers':{'SS_P2B':'9','SS_PDB':'10','NCPU':'8'}}
  settings={};args=[]
  for label,env in variants.items():
   args.append(label+'='+str(e/'bin'/'diag-candidate'));settings[label]=env
  path=e/'results'/('cache-'+str(seed)+'-'+str(events)+'.json')
  run(['python','tools/bench.py','--data',data,'--warmups','1','--runs',repeats,'--threads',threads,'--affinity',','.join(map(str,cpus)),'--variants',*args,'--variant-env',json.dumps(settings),'--result',path])
  rows=json.loads(path.read_text());study={'events':events,'seed':seed,'input_csv_bytes':sum(p.stat().st_size for p in data.rglob('*.csv')),'manifest':json.loads((data/'manifest.json').read_text()),'variants':{}};report['studies'][str(seed)+'-'+str(events)]=study;save()
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
   run(['python','tools/check.py',e/'bench-out'/label,'--other',e/'bench-out/auto']);record['exact_output']='passed';save()
  report['studies'][str(seed)+'-'+str(events)]=study;save();shutil.rmtree(data)
  for label in variants:shutil.rmtree(e/'bench-out'/label)
 assert len(report['studies'])==3
 report['success']=True
except Exception as error:report['error']=str(error)[:1000];raise
finally:save()
