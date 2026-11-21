"""Publish an existing local commit via GitHub Git Data API when Git HTTPS fails.
Creates content-addressed objects; verifies the full tree; never force-updates refs.
"""
import argparse,datetime,json,pathlib,subprocess
p=argparse.ArgumentParser();p.add_argument('--repo',required=True);p.add_argument('--commit',default='HEAD');p.add_argument('--branch',required=True);p.add_argument('--update',action='store_true');p.add_argument('--receipt',default='submission/PUSH.json');a=p.parse_args()
r=pathlib.Path(__file__).resolve().parents[1];tmp=r/'experiments/candidates/api-request.json';tmp.parent.mkdir(exist_ok=True)
def git(*args):return subprocess.check_output(['git',*args],cwd=r)
def api(method,endpoint,payload=None):
 cmd=['gh','api','--method',method,'repos/'+a.repo+'/'+endpoint]
 if payload is not None:tmp.write_text(json.dumps(payload,ensure_ascii=False));cmd+=['--input',str(tmp)]
 q=subprocess.run(cmd,capture_output=True,text=True)
 if q.returncode:raise RuntimeError(q.stderr.strip()[:1500])
 return json.loads(q.stdout)
commit=git('rev-parse',a.commit).decode().strip();parents=git('show','-s','--format=%P',commit).decode().strip().split();assert len(parents)==1
parent=parents[0];expected=git('rev-parse',commit+'^{tree}').decode().strip();tree=git('rev-parse',parent+'^{tree}').decode().strip()
entries={}
for rec in git('ls-tree','-r','-z',commit).split(b'\0'):
 if not rec:continue
 meta,path=rec.split(b'\t',1);mode,typ,sha=meta.decode().split();entries[path.decode()]=(mode,typ,sha)
changes=git('diff-tree','--no-commit-id','--name-status','-r','-z',parent,commit).decode().split('\0');items=[]
for i in range(0,len(changes)-1,2):
 status,path=changes[i:i+2]
 if status=='D':items.append(dict(path=path,mode='100644',type='blob',sha=None));continue
 mode,typ,sha=entries[path];content=git('cat-file','blob',sha).decode('utf-8');items.append(dict(path=path,mode=mode,type=typ,content=content))
batches=[];batch=[];size=0
for item in items:
 n=len(json.dumps(item,ensure_ascii=False).encode())
 if batch and size+n>600000:batches.append(batch);batch=[];size=0
 batch.append(item);size+=n
if batch:batches.append(batch)
for i,batch in enumerate(batches):
 tree=api('POST','git/trees',dict(base_tree=tree,tree=batch))['sha'];print(f'tree batch {i+1}/{len(batches)}: {len(batch)} files',flush=True)
assert tree==expected,(tree,expected)
meta=git('show','-s','--format=%an%n%ae%n%aI%n%cn%n%ce%n%cI',commit).decode().splitlines();raw=git('cat-file','commit',commit);message=raw.split(b'\n\n',1)[1].decode()
payload=dict(message=message,tree=tree,parents=[parent],author=dict(name=meta[0],email=meta[1],date=meta[2]),committer=dict(name=meta[3],email=meta[4],date=meta[5]))
result=api('POST','git/commits',payload);remote=result['sha'];assert result['tree']['sha']==expected
if a.update:
 current=api('GET','git/ref/heads/'+a.branch)['object']['sha'];assert current==parent,(current,parent)
 api('PATCH','git/refs/heads/'+a.branch,dict(sha=remote,force=False))
else:api('POST','git/refs',dict(ref='refs/heads/'+a.branch,sha=remote))
receipt=dict(local_commit=commit,remote_commit=remote,tree=tree,tree_verified=True,branch=a.branch,repository=a.repo,url='https://github.com/'+a.repo+'/commit/'+remote,commit_metadata_identical=remote==commit)
(r/a.receipt).write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
