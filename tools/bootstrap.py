"""Download public official assets and generate ignored local test inputs.
Run from a fresh checkout with Rust/cargo, pyarrow and polars installed.
"""
import os,pathlib,subprocess,urllib.request,zipfile
root=pathlib.Path(__file__).resolve().parents[1];os.chdir(root)
a=root/'experiments/assets';a.mkdir(parents=True,exist_ok=True)
url='https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/'
for name,folder in [('generator.zip','generator'),('starter-polars.zip','starter'),('dev-answer.zip','answer')]:
 path=a/name
 if not path.exists():urllib.request.urlretrieve(url+name,path)
 if not (a/folder).exists():
  with zipfile.ZipFile(path) as z:z.extractall(a/folder)
if not (a/'rules.md').exists():urllib.request.urlretrieve(url+'rules.md',a/'rules.md')
subprocess.run(['cargo','build','--release','--locked','--manifest-path',str(a/'generator/generator/Cargo.toml')],check=True)
b=root/'experiments/bin';b.mkdir(exist_ok=True);import shutil
shutil.copy2(a/'generator/generator/target/release/generate',b/'generate')
for seed,events,clicks,name in [(1,1000000,250000,'dev'),(314,20000,5000,'small')]:
 if not (root/f'experiments/data-{name}').exists():subprocess.run([str(b/'generate'),'--seed',str(seed),'--events',str(events),'--clicks',str(clicks),'--threads','2','--output',f'experiments/data-{name}'],check=True)
print('Official assets and local dev/small inputs ready; none are versioned.')
