"""Independent same-artifact thread/partition sweeps, with full output checks."""
import datetime,hashlib,json,os,pathlib,shutil,statistics,subprocess,sys,time,urllib.request,zipfile
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r)
e=r/'experiments';(e/'results').mkdir(parents=True,exist_ok=True);(e/'bin').mkdir(exist_ok=True)
result=e/'results/scaling-result.json';target=os.environ['SOURCE_SHA'];repo=os.environ.get('GITHUB_REPOSITORY','akshayagg18/shopping-cl')
if '--publish' in sys.argv:
 report=json.loads(result.read_text()) if result.exists() else {'success':False,'error':'No report created'}
 summary='Same-artifact hosted study, **not an official score**.\n\n```json\n'+json.dumps(report,indent=2)+'\n```'
 assert len(summary)<60000,len(summary)
 payload={'name':'Codex scaling and partition study','head_sha':target,'status':'completed','conclusion':'success' if report.get('success') else 'failure','output':{'title':'Thread and partition measurements','summary':summary}}
 path=e/'check-scaling.json';path.write_text(json.dumps(payload));subprocess.run(['gh','api','--method','POST','repos/'+repo+'/check-runs','--input',str(path)],check=True);raise SystemExit()
report={'success':False,'source':target,'official_score':False,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'run_url':'https://github.com/'+repo+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID',''),'studies':{},'profiles':{}}
def save():result.write_text(json.dumps(report,indent=2)+'\n')
def run(args,**kw):
 args=list(map(str,args));p=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,**kw);print(p.stdout,flush=True)
 if p.returncode:report['last_failed_output']=p.stdout[-9000:];save();raise subprocess.CalledProcessError(p.returncode,args)
 return p.stdout
def read(path):
 p=pathlib.Path(path);return p.read_text().strip() if p.exists() else 'unavailable'
def stats(path):
 rows=json.loads(path.read_text());out={}
 for label in dict.fromkeys(x['variant'] for x in rows):
  rs=[x for x in rows if x['variant']==label];xs=[x['wall_ms'] for x in rs]
  out[label]={'n':len(xs),'median_ms':statistics.median(xs),'mean_ms':statistics.mean(xs),'sd_ms':statistics.stdev(xs),'min_ms':min(xs),'max_ms':max(xs),'samples_ms':xs,'user_ms':statistics.median(x['user_ms'] for x in rs),'system_ms':statistics.median(x['sys_ms'] for x in rs),'peak_rss_kib':statistics.median(x['peak_rss_kib'] for x in rs),'settings':rs[0].get('variant_environment',{})}
 return out
outputs=[]
def bench(name,variants,settings,runs=7,profile=False):
 path=e/'results'/('scaling-'+name+'.json');args=['python','tools/bench.py','--data',data,'--warmups','1','--variants']+[label+'='+str(binary) for label,binary in variants]+['--variant-env',json.dumps(settings),'--runs',runs,'--threads',len(cpus),'--affinity',','.join(map(str,cpus)),'--result',path]
 if profile:args+=['--profile']
 run(args);outputs.extend(label for label,_ in variants);return stats(path),path
try:
 cpus=sorted(os.sched_getaffinity(0));model=next(l.split(':',1)[1].strip() for l in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('model name'))
 flags=next(l.split(':',1)[1].strip().split() for l in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('flags'))
 topology={}
 for cpu in cpus:
  base=pathlib.Path('/sys/devices/system/cpu')/('cpu'+str(cpu));topology[cpu]={'core_id':read(base/'topology/core_id'),'package':read(base/'topology/physical_package_id'),'thread_siblings':read(base/'topology/thread_siblings_list'),'caches':[]}
  for cache in sorted((base/'cache').glob('index*')):topology[cpu]['caches'].append({k:read(cache/k) for k in ['level','type','size','shared_cpu_list']})
 report['hardware']={'model':model,'affinity':cpus,'cpu_max':read('/sys/fs/cgroup/cpu.max'),'memory_max':read('/sys/fs/cgroup/memory.max'),'topology':topology,'features':{f:f in flags for f in ['avx2','avx512f','avx512bw','avx512_vbmi2','pclmulqdq','bmi2']}};save()
 context=e/'context-source';context.mkdir(exist_ok=True);archive=e/'source.tar'
 with archive.open('wb') as h:subprocess.run(['git','archive',target],stdout=h,check=True)
 run(['tar','-xf',archive,'-C',context]);archive.unlink()
 start=time.monotonic();run(['docker','build','--platform','linux/amd64','-t','shop-study',context]);report['docker_build_s']=time.monotonic()-start
 cid=run(['docker','create','shop-study']).strip().splitlines()[-1];binary=e/'bin/study';run(['docker','cp',cid+':/ssp',binary]);run(['docker','rm',cid]);report['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest();save()
 profile_env=dict(os.environ,CARGO_TARGET_DIR=str(e/'profile-target'),RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+crt-static,+pclmulqdq,+aes')
 run(['cargo','build','--release','--locked','--features','diagnostics','--manifest-path',context/'Cargo.toml','--target','x86_64-unknown-linux-gnu'],env=profile_env)
 profile_binary=e/'bin/study-profile';shutil.copy2(e/'profile-target/x86_64-unknown-linux-gnu/release/ssp',profile_binary)
 assets=e/'assets';assets.mkdir(exist_ok=True);z=assets/'generator.zip';urllib.request.urlretrieve('https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip',z)
 report['generator_sha256']=hashlib.sha256(z.read_bytes()).hexdigest()
 with zipfile.ZipFile(z) as f:f.extractall(assets/'generator')
 run(['cargo','build','--release','--locked','--manifest-path',assets/'generator/generator/Cargo.toml'])
 data=e/'data-study';run([assets/'generator/generator/target/release/generate','--seed','2026','--events','20000000','--clicks','5000000','--threads',len(cpus),'--output',data]);report['input_csv_bytes']=sum(p.stat().st_size for p in data.rglob('*.csv'))
 variants=[('threads'+str(n),binary) for n in [1,2,4,8]];settings={label:{'NCPU':label[7:]} for label,_ in variants}
 report['studies']['threads'],_=bench('threads',variants,settings);save()
 variants=[('dedup'+str(n),binary) for n in [7,8,9,10,11]];settings={label:{'SS_PDB':label[5:]} for label,_ in variants}
 report['studies']['dedup'],_=bench('dedup',variants,settings);save()
 variants=[('person'+str(n),binary) for n in [6,7,8,9,10,11]];settings={label:{'SS_P2B':label[6:]} for label,_ in variants}
 report['studies']['person'],_=bench('person',variants,settings);save()
 for n in [4,8]:
  label='profile'+str(n);measurements,path=bench(label,[(label,profile_binary)],{label:{'NCPU':str(n)}},runs=3,profile=True)
  summary=e/'results'/('stage-'+label+'.json');run(['python','tools/analyze_profile.py',path,'--variant',label,'--output',summary]);report['profiles'][label]={'measurements':measurements[label],'summary':json.loads(summary.read_text())};save()
 # Exact canonical strings, once per final output of every setting. Sorting is
 # outside all timed intervals. DATE/logical schema is checked by this reader.
 from check import lines_from_parquet
 baseline={t:sorted(lines_from_parquet(str(e/'bench-out/threads4'),t)) for t in ['user_daily','campaign_daily']}
 report['output_comparisons']={}
 for label in dict.fromkeys(outputs):
  for table,expected in baseline.items():assert sorted(lines_from_parquet(str(e/'bench-out'/label),table))==expected,(label,table)
  report['output_comparisons'][label]='both tables exact'
 report['success']=True
except Exception as exc:report['error']=str(exc)[:1500];raise
finally:save()
