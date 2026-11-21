"""Additional contract cases: large fields, CR, UTF-8, earliest invalid copy.
Also exercises records crossing the parser's 192KiB and 4MiB block boundaries.
"""
import json,os,pathlib,shutil,subprocess,sys
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r)
binary=str(pathlib.Path(sys.argv[1]).resolve());data=pathlib.Path('experiments/data-adversarial');out=pathlib.Path('experiments/reference-adversarial');shutil.rmtree(data,ignore_errors=True);shutil.rmtree(out,ignore_errors=True)
for d in ['events','clicks','links']:(data/d).mkdir(parents=True)
(data/'manifest.json').write_text(json.dumps({'window_start':'2026-10-01T00:00:00Z','window_end':'2026-10-15T00:00:00Z'}))
(data/'products.csv').write_text('product_id,category\np1,books\np2,shoes\n');(data/'campaigns.csv').write_text('campaign_id,channel\nc1,search\nc2,social\n')
long='a'*65534+'é'+'z';alias='alias';(data/'links'/'0.csv').write_text('user_id_a,user_id_b\n'+long+','+alias+'\n')
head='event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n'
evs=['ancient,u4,bad,1900-01-01T00:00:00Z,view,p1,','ancient,u4,2026-10-03T09:00:00Z,1899-01-01T00:00:00Z,view,p1,','pre-epoch,u4,2026-10-03T08:00:00Z,1900-01-01T00:00:00Z,view,p1,','pre-epoch,u5,2026-10-03T08:00:00Z,1899-01-01T00:00:00Z,view,p1,','e1,alias,2026-10-03T23:50:00Z,2026-10-03T23:50:00Z,purchase,p1,500','e2,alias,2026-10-04T00:20:00Z,2026-10-04T00:20:00Z,purchase,p2,500','dup,u2,bad,2026-10-03T10:00:00Z,view,p1,','dup,u2,2026-10-03T09:00:00Z,2026-10-03T10:01:00Z,view,p1,']
# Long legal ID and full UTF-8 name; repeated no-purchase rows tie timestamps.
evs.extend(['unicode1,unique-é-one,2026-10-03T08:00:00Z,2026-10-03T08:01:00Z,purchase,p1,250','unicode2,unique-é-two,2026-10-03T08:00:00Z,2026-10-03T08:01:00Z,view,p1,','long-unlinked,'+long+'-unlinked,2026-10-03T08:00:00Z,2026-10-03T08:01:00Z,view,p1,'])
long_event='event-'+('é'*40000)
evs.extend([long_event+',u7,bad,2026-10-03T10:00:00Z,view,p1,',long_event+',u7,2026-10-03T09:00:00Z,2026-10-03T10:01:00Z,view,p1,'])
for i in range(90000):evs.append(f'x{i},u3,1791021600000,2026-10-03T10:01:00Z,view,p1,')
(data/'events'/'0.csv').write_text(head+'\n'.join(evs)+'\n');(data/'events'/'1.csv').write_text(head+'q,'+long+',2026-10-06T10:00:00Z,2026-10-06T10:00:00Z,view,p1,')
(data/'clicks'/'0.csv').write_text('click_id,user_id,campaign_id,ts\nz,alias,c2,2026-10-03T23:50:00Z\na,alias,c1,2026-10-03T23:50:00Z\n')
subprocess.run([binary],env=dict(os.environ,DATA_DIR=str(data),OUTPUT_DIR=str(out),NCPU=os.environ.get('NCPU','2')),check=True)
subprocess.run(['python','tools/ref.py',str(data),'experiments/reference-adversarial'],check=True)
subprocess.run(['python','tools/check.py',str(out),'--txt','experiments/reference-adversarial'],check=True)
print('adversarial OK')
