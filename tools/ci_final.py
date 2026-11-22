"""Fresh hosted-runner validation; no datasets or answers are versioned.
Only standard public ubuntu-latest is requested. Results are also a check output
so reading them does not depend on the separate Actions blob-download service.
"""
import datetime,hashlib,json,os,pathlib,shutil,statistics,subprocess,sys,time,urllib.request,zipfile
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r)
e=r/'experiments';e.mkdir(exist_ok=True);(e/'results').mkdir(exist_ok=True);(e/'bin').mkdir(exist_ok=True)
result=e/'results/final-hosted-result.json';target=os.environ['CANDIDATE_SHA']
repo=os.environ.get('GITHUB_REPOSITORY','akshayagg18/shopping-cl')
if '--publish' in sys.argv:
 report=json.loads(result.read_text()) if result.exists() else {'success':False,'error':'Runner failed before writing its receipt.'}
 summary='Independent hosted-runner measurement; **not an official competition score**.\n\n```json\n'+json.dumps(report,indent=2)+'\n```'
 assert len(summary)<60000,len(summary)
 payload={'name':'Codex combined 20M/60M validation','head_sha':target,'status':'completed','conclusion':'success' if report.get('success') else 'failure','output':{'title':'Hosted validation result','summary':summary}}
 path=e/'check-request.json';path.write_text(json.dumps(payload))
 subprocess.run(['gh','api','--method','POST','repos/'+repo+'/check-runs','--input',str(path)],check=True)
 raise SystemExit()
def run(args,**kw):
 args=list(map(str,args))
 if 'stdout' in kw:return subprocess.run(args,check=True,**kw)
 process=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,**kw)
 print(process.stdout,flush=True)
 if process.returncode:
  report['last_failed_output']=process.stdout[-12000:];save()
  raise subprocess.CalledProcessError(process.returncode,args)
 return process
def capture(args):return subprocess.check_output(list(map(str,args)),text=True)
def save():result.write_text(json.dumps(report,indent=2)+'\n')
report={'success':False,'candidate':target,'baseline':'ae0cf75dfbcc7eead8c68e20b09ad4ad6cb962c2','official_score':False,'run_url':'https://github.com/'+repo+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID',''),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'prior':'82d185b15affaf4b65fcb4936d23d410478debfc','measurements':{}}
try:
 cpus=sorted(os.sched_getaffinity(0));threads=len(cpus)
 def optional(path):
  p=pathlib.Path(path);return p.read_text().strip() if p.exists() else 'unavailable'
 report['hardware']={'affinity':cpus,'cpu_model':next(x.split(':',1)[1].strip() for x in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')),'memory':{k:v.strip() for k,v in (line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines()) if k in ['MemTotal','SwapTotal','SwapFree']},'cpu_max':optional('/sys/fs/cgroup/cpu.max'),'memory_max':optional('/sys/fs/cgroup/memory.max')}
 report['topology']=capture(['lscpu','-e=CPU,CORE,SOCKET,NODE,CACHE'])
 save()
 contexts={}
 for label,commit in [('baseline',report['baseline']),('prior',report['prior']),('candidate',target)]:
  context=e/('context-'+label);context.mkdir(exist_ok=True)
  archive=e/(label+'.tar')
  with archive.open('wb') as h:run(['git','archive',commit],stdout=h)
  run(['tar','-xf',archive,'-C',context]);archive.unlink();contexts[label]=context
 # Both exact source trees use the candidate's pinned compiler/static Dockerfile.
 for label,context in contexts.items():
  for name in ['Dockerfile','LICENSE']:
   shutil.copy2(contexts['candidate']/name,context/name) if label!='candidate' else None
  (context/'.dockerignore').write_text('**\n!Dockerfile\n!Cargo.toml\n!Cargo.lock\n!LICENSE\n!src/\n!src/**\n')
  t=time.monotonic();run(['docker','build','--platform','linux/amd64','-t','shop-ci-'+label,context]);report[label+'_build_s']=time.monotonic()-t
  cid=capture(['docker','create','shop-ci-'+label]).strip()
  run(['docker','cp',cid+':/ssp',e/'bin'/('hosted-'+label)]);run(['docker','rm',cid])
  report[label+'_image']=json.loads(capture(['docker','image','inspect','shop-ci-'+label]))[0]['Id']
 # The hosted runner is dedicated to this job; only its disposable build cache is removed.
 run(['docker','builder','prune','--all','--force']);save()
 assets=e/'assets';assets.mkdir(exist_ok=True)
 root='https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/'
 for name,folder in [('generator.zip','generator'),('starter-polars.zip','starter'),('dev-answer.zip','answer')]:
  z=assets/name;urllib.request.urlretrieve(root+name,z)
  with zipfile.ZipFile(z) as f:f.extractall(assets/folder)
 report['asset_sha256']={name:hashlib.sha256((assets/name).read_bytes()).hexdigest() for name in ['generator.zip','starter-polars.zip','dev-answer.zip']}
 report['generator_zip_sha256']=report['asset_sha256']['generator.zip']
 assert report['generator_zip_sha256']=='78f973f75d14b48e792628284f19c75de71fa597d20952e16a9ced93c78bee4a', 'Generator changed again: refuse cross-version evidence'
 assert report['asset_sha256']['dev-answer.zip']=='c2439e0e2e7456d721852814e9928e077648803d5c57a1760a40c7d3b0864d20'
 urllib.request.urlretrieve(root+'rules.md',assets/'rules.md')
 report['rules_sha256']=hashlib.sha256((assets/'rules.md').read_bytes()).hexdigest();save()
 manifest=assets/'generator/generator/Cargo.toml';run(['cargo','build','--release','--locked','--manifest-path',manifest])
 gen=assets/'generator/generator/target/release/generate'
 def generate(seed,events,name):
  data=e/('data-'+name);run([gen,'--seed',seed,'--events',events,'--clicks',events//4,'--threads',threads,'--output',data]);return data
 def docker_run(label,data,out):
  out.mkdir(exist_ok=True);out.chmod(0o777)
  for name in ['user_daily','campaign_daily']:
   folder=out/name;folder.mkdir(exist_ok=True);folder.chmod(0o777)
   for old in folder.glob('*.parquet'):old.unlink()
  started=time.monotonic()
  process=run(['docker','run','--rm','--network','none','--read-only','--user','1000:1000','--cpus',threads,'--memory','14g','--memory-swap','14g','--tmpfs','/tmp:rw,size=64m','-e','NCPU='+str(threads),'-e','SS_TIMING=1','-v',str(data)+':/data:ro','-v',str(out)+':/output','shop-ci-'+label])
  import re
  laps={name:float(ms) for ms,name in re.findall(r'^\s*([0-9.]+) ms\s+(\S+)\s*$',process.stdout,re.M)}
  report.setdefault('sandbox_runs',[]).append({'variant':label,'dataset':data.name,'docker_cli_wall_ms':(time.monotonic()-started)*1000,'internal_laps_ms':laps,'note':'Fresh container; CLI wall includes daemon startup/cleanup and is not official timing.'});save()
 dev=generate(1,1_000_000,'dev')
 for label in contexts:
  out=e/('docker-dev-'+label);docker_run(label,dev,out)
  run(['python','tools/check.py',out,'--expected',assets/'answer/dev-answer'])
 for label in contexts:
  run(['python',assets/'starter/starter-polars/scripts/run_fixtures.py',assets/'starter/starter-polars/fixtures','--','python','tools/run_image.py','shop-ci-'+label])
 report['dev_and_36_fixtures']='passed';save()
 for seed,events,name,repeats in [(20261123,20_000_000,'20m',7),(20261123,60_000_000,'60m',3),(20261124,60_000_000,'60m-fresh',3)]:
  data=generate(seed,events,name);path=e/'results'/('hosted-'+name+'.json')
  run(['python','tools/bench.py','--data',data,'--warmups','1','--runs',repeats,'--threads',threads,'--affinity',','.join(map(str,cpus)),'--variants','hosted-baseline='+str(e/'bin/hosted-baseline'),'hosted-prior='+str(e/'bin/hosted-prior'),'hosted-candidate='+str(e/'bin/hosted-candidate'),'--result',path])
  run(['python','tools/check.py',e/'bench-out/hosted-candidate','--other',e/'bench-out/hosted-baseline'])
  run(['python','tools/check.py',e/'bench-out/hosted-prior','--other',e/'bench-out/hosted-baseline'])
  measurements={}
  rows=json.loads(path.read_text())
  for variant in ['hosted-baseline','hosted-prior','hosted-candidate']:
   rs=[x for x in rows if x['variant']==variant];xs=[x['wall_ms'] for x in rs]
   measurements[variant]={'n':len(xs),'median_ms':statistics.median(xs),'mean_ms':statistics.mean(xs),'sd_ms':statistics.stdev(xs),'min_ms':min(xs),'max_ms':max(xs),'samples_ms':xs,'peak_rss_kib':statistics.median(x['peak_rss_kib'] for x in rs),'user_ms':statistics.median(x['user_ms'] for x in rs),'system_ms':statistics.median(x['sys_ms'] for x in rs),'major_faults':statistics.median(x['major_faults'] for x in rs),'minor_faults':statistics.median(x['minor_faults'] for x in rs),'voluntary_cs':statistics.median(x['voluntary_cs'] for x in rs),'involuntary_cs':statistics.median(x['involuntary_cs'] for x in rs)}
  import re
  for variant in ['hosted-baseline','hosted-prior','hosted-candidate']:
   stage_runs=[]
   for row in [x for x in rows if x['variant']==variant]:
    last=0;stages={}
    for ms,stage_name in re.findall(r'^\s*([0-9.]+) ms\s+(\S+)\s*$',(r/row['log']).read_text(),re.M):
     current=float(ms);stages[stage_name]=current-last;last=current
    stage_runs.append(stages)
   measurements[variant]['stage_median_ms']={name:statistics.median(stages[name] for stages in stage_runs if name in stages) for name in {k for stages in stage_runs for k in stages}}
   measurements[variant]['binary_sha256']=hashlib.sha256((e/'bin'/variant).read_bytes()).hexdigest()
  measurements['seed']=seed;measurements['manifest']=json.loads((data/'manifest.json').read_text())
  measurements['exact_output_comparison']='passed' ;measurements['input_csv_bytes']=sum(p.stat().st_size for p in data.rglob('*.csv'))
  for repeat in range(3):
   labels=list(contexts) if repeat%2==0 else list(contexts)[::-1]
   for label in labels:docker_run(label,data,e/('docker-'+name+'-'+label))
  for label in contexts:
   sandbox=e/('docker-'+name+'-'+label)
   run(['python','tools/check.py',sandbox,'--other',e/'bench-out/hosted-baseline']);shutil.rmtree(sandbox)
  measurements['all_sandbox_comparisons']='passed';report['measurements'][name]=measurements;save()
  shutil.rmtree(data)
 assert set(report['measurements'])=={'20m','60m','60m-fresh'}, 'Dataset summaries must survive stage parsing'
 report['success']=True
except Exception as error:
 report['error']=str(error)[:1500];raise
finally:save()
