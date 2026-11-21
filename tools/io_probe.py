"""Measure cached full-input scans and output writes; record a reproducibility digest."""
import hashlib,json,pathlib,statistics,subprocess,sys,time
r=pathlib.Path(__file__).resolve().parents[1];data=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2]);files=sorted(data.rglob('*.csv'))
rows=[];bytes_in=sum(p.stat().st_size for p in files);h=hashlib.sha256();counts={}
for p in files:
 count=0;h.update(str(p.relative_to(data)).encode()+b'\0')
 with p.open('rb') as f:
  while chunk:=f.read(8<<20):h.update(chunk);count+=chunk.count(b'\n')
 folder=p.parent.name if p.parent!=data else p.name
 c=counts.setdefault(folder,dict(files=0,bytes=0,physical_data_lines=0));c['files']+=1;c['bytes']+=p.stat().st_size;c['physical_data_lines']+=max(0,count-1)
for i in range(7):
 d=json.loads(subprocess.check_output([str(r/'experiments/bin/bandwidth'),*map(str,files)],text=True));d['input_bytes']=bytes_in;d['gb_s']=bytes_in/d['seconds']/1e9;rows.append(d)
output_files=sorted(out.glob('*/*.parquet'));blob=b''.join(f.read_bytes() for f in output_files);writes=[];temp=r/'experiments/bench-out/io-test'
for i in range(7):
 start=time.perf_counter()
 with temp.open('wb') as f:f.write(blob)
 elapsed=time.perf_counter()-start;writes.append(dict(seconds=elapsed,gb_s=len(blob)/elapsed/1e9))
temp.unlink()
res=dict(dataset=str(data),csv_sha256=h.hexdigest(),csv_bytes=bytes_in,counts=counts,manifest=json.loads((data/'manifest.json').read_text()),input=rows,output_bytes=len(blob),output=writes)
(r/'experiments/results/io-large.json').write_text(json.dumps(res,indent=2)+'\n');print('input bytes',bytes_in,'output bytes',len(blob),'read GB/s',statistics.median(d['gb_s'] for d in rows),'write GB/s',statistics.median(d['gb_s'] for d in writes))
