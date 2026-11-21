"""Fresh hosted-runner validation; no datasets or answers are versioned.
Only standard public ubuntu-latest is requested. Results are also a check output
so reading them does not depend on the separate Actions blob-download service.
"""
import datetime,hashlib,json,os,pathlib,shutil,statistics,subprocess,sys,time,urllib.request,zipfile
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r)
e=r/'experiments';e.mkdir(exist_ok=True);(e/'results').mkdir(exist_ok=True);(e/'bin').mkdir(exist_ok=True)
result=e/'results/hosted-result.json';target=os.environ['CANDIDATE_SHA']
repo=os.environ.get('GITHUB_REPOSITORY','akshayagg18/shopping-cl')
if '--publish' in sys.argv:
 report=json.loads(result.read_text()) if result.exists() else {'success':False,'error':'Runner failed before writing its receipt.'}
 summary='Independent hosted-runner measurement; **not an official competition score**.\n\n```json\n'+json.dumps(report,indent=2)+'\n```'
 payload={'name':'Codex measured 20M/60M validation','head_sha':target,'status':'completed','conclusion':'success' if report.get('success') else 'failure','output':{'title':'Hosted validation result','summary':summary[:60000]}}
 path=e/'check-request.json';path.write_text(json.dumps(payload))
 subprocess.run(['gh','api','--method','POST','repos/'+repo+'/check-runs','--input',str(path)],check=True)
 raise SystemExit()
def run(args,**kw):return subprocess.run(list(map(str,args)),check=True,**kw)
def capture(args):return subprocess.check_output(list(map(str,args)),text=True)
def save():result.write_text(json.dumps(report,indent=2)+'\n')
report={'success':False,'candidate':target,'baseline':'d2dc441e6bed67a57572a7c1f12b06dad7f163cc','official_score':False,'run_url':'https://github.com/'+repo+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID',''),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'measurements':{}}
try:
 cpus=sorted(os.sched_getaffinity(0));threads=len(cpus)
 def optional(path):
  p=pathlib.Path(path);return p.read_text().strip() if p.exists() else 'unavailable'
 report['hardware']={'affinity':cpus,'cpu_model':next(x.split(':',1)[1].strip() for x in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')),'memory':{k:v.strip() for k,v in (line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines()) if k in ['MemTotal','SwapTotal','SwapFree']},'cpu_max':optional('/sys/fs/cgroup/cpu.max'),'memory_max':optional('/sys/fs/cgroup/memory.max')}
 save()
 contexts={}
 for label,commit in [('baseline',report['baseline']),('candidate',target)]:
  context=e/('context-'+label);context.mkdir(exist_ok=True)
  archive=e/(label+'.tar')
  with archive.open('wb') as h:run(['git','archive',commit],stdout=h)
  run(['tar','-xf',archive,'-C',context]);archive.unlink();contexts[label]=context
 # Both exact source trees use the candidate's pinned compiler/static Dockerfile.
 for label,context in contexts.items():
  for name in ['Dockerfile','LICENSE']:
   shutil.copy2(contexts['candidate']/name,context/name) if label=='baseline' else None
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
 report['generator_zip_sha256']=hashlib.sha256((assets/'generator.zip').read_bytes()).hexdigest()
 manifest=assets/'generator/generator/Cargo.toml';run(['cargo','build','--release','--locked','--manifest-path',manifest])
 gen=assets/'generator/generator/target/release/generate'
 def generate(seed,events,name):
  data=e/('data-'+name);run([gen,'--seed',seed,'--events',events,'--clicks',events//4,'--threads',threads,'--output',data]);return data
 def docker_run(label,data,out):
  out.mkdir(exist_ok=True);out.chmod(0o777)
  for name in ['user_daily','campaign_daily']:(out/name).mkdir(exist_ok=True);(out/name).chmod(0o777)
  run(['docker','run','--rm','--network','none','--read-only','--user','1000:1000','--cpus',threads,'--memory','14g','--memory-swap','14g','--tmpfs','/tmp:rw,size=64m','-e','NCPU='+str(threads),'-e','SS_TIMING=1','-v',str(data)+':/data:ro','-v',str(out)+':/output','shop-ci-'+label])
 dev=generate(1,1_000_000,'dev')
 for label in contexts:
  out=e/('docker-dev-'+label);docker_run(label,dev,out)
  run(['python','tools/check.py',out,'--expected',assets/'answer/dev-answer'])
 run(['python',assets/'starter/starter-polars/scripts/run_fixtures.py',assets/'starter/starter-polars/fixtures','--','python','tools/run_image.py','shop-ci-candidate'])
 report['dev_and_36_fixtures']='passed';save()
 for seed,events,name,repeats in [(2026,20_000_000,'20m',7),(20261122,60_000_000,'60m',3)]:
  data=generate(seed,events,name);path=e/'results'/('hosted-'+name+'.json')
  run(['python','tools/bench.py','--data',data,'--warmups','1','--runs',repeats,'--threads',threads,'--affinity',','.join(map(str,cpus)),'--variants','hosted-baseline='+str(e/'bin/hosted-baseline'),'hosted-candidate='+str(e/'bin/hosted-candidate'),'--result',path])
  run(['python','tools/check.py',e/'bench-out/hosted-candidate','--other',e/'bench-out/hosted-baseline'])
  measurements={}
  rows=json.loads(path.read_text())
  for variant in ['hosted-baseline','hosted-candidate']:
   rs=[x for x in rows if x['variant']==variant];xs=[x['wall_ms'] for x in rs]
   measurements[variant]={'n':len(xs),'median_ms':statistics.median(xs),'mean_ms':statistics.mean(xs),'sd_ms':statistics.stdev(xs),'min_ms':min(xs),'max_ms':max(xs),'samples_ms':xs,'peak_rss_kib':statistics.median(x['peak_rss_kib'] for x in rs),'user_ms':statistics.median(x['user_ms'] for x in rs),'system_ms':statistics.median(x['sys_ms'] for x in rs)}
  measurements['exact_output_comparison']='passed';measurements['input_csv_bytes']=sum(p.stat().st_size for p in data.rglob('*.csv'))
  sandbox=e/('docker-'+name);docker_run('candidate',data,sandbox)
  run(['python','tools/check.py',sandbox,'--other',e/'bench-out/hosted-baseline'])
  measurements['candidate_sandbox_comparison']='passed';report['measurements'][name]=measurements;save()
  shutil.rmtree(data);shutil.rmtree(sandbox)
 report['success']=True
except Exception as error:
 report['error']=str(error)[:1500];raise
finally:save()
