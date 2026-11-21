"""Reproduce official fixtures/dev answer and literal-reference fuzz comparisons."""
import argparse, os, pathlib, subprocess, shutil
p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--threads',type=int,default=2);p.add_argument('--fuzz-rounds',type=int,default=5);a=p.parse_args()
r=pathlib.Path(__file__).resolve().parents[1];os.chdir(r);binary=str(pathlib.Path(a.binary).resolve());env=dict(os.environ,NCPU=str(a.threads),POLARS_MAX_THREADS='2')
def run(*args, **kw):subprocess.run(list(args),env=env,check=True,**kw)
run('python','experiments/assets/starter/starter-polars/scripts/run_fixtures.py','experiments/assets/starter/starter-polars/fixtures','--',binary)
for k in range(a.fuzz_rounds):
 data='experiments/fuzz-'+str(k);out='experiments/reference-out';prefix='experiments/reference-'+str(k)
 if k:run('python','tools/fuzz.py','experiments/data-small',data,str(k))
 else:data='experiments/data-small'
 shutil.rmtree(out,ignore_errors=True)
 subprocess.run([binary],env=dict(env,DATA_DIR=data,OUTPUT_DIR=out),check=True)
 run('python','tools/ref.py',data,prefix)
 run('python','tools/check.py',out,'--txt',prefix)
shutil.rmtree('experiments/reference-dev',ignore_errors=True)
subprocess.run([binary],env=dict(env,DATA_DIR='experiments/data-dev',OUTPUT_DIR='experiments/reference-dev'),check=True)
run('python','tools/check.py','experiments/reference-dev','--expected','experiments/assets/answer/dev-answer')
