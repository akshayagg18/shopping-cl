"""Archive later source experiments as patches against the frozen commit."""
import difflib,pathlib,subprocess
r=pathlib.Path(__file__).resolve().parents[1];c=r/'experiments/candidates';out=r/'experiments/patches';out.mkdir(exist_ok=True)
base='d2dc441e6bed67a57572a7c1f12b06dad7f163cc'
def original(p):
 x=subprocess.run(['git','show',base+':'+p],cwd=r,capture_output=True);return x.stdout.decode() if x.returncode==0 else ''
oldout=original('src/output.rs').replace('    let f = BufWriter','    let span = crate::profile::Span::new("parquet_encode", batch.num_rows());\n    let rows = batch.num_rows();\n    let f = BufWriter').replace('    w.close().expect("close");','    w.close().expect("close");\n    span.finish(rows, std::fs::metadata(path).map_or(0, |m| m.len() as usize));').replace('            let n = a.rows.len();','            let span = crate::profile::Span::new("arrow_construct", i);\n            let n = a.rows.len();').replace('            write_batch(&ud.join','            span.finish(n, bytes);\n            write_batch(&ud.join')
encoded={'encoded-persons','encoded-p1-1024','zero-gather','generic-ingest','grouped8','click24'}
for path in sorted(c.glob('*.rs')):
 if path.name.startswith('output-'):continue
 files={'src/main.rs':path.read_text(),'src/parse.rs':(r/'src/parse.rs').read_text(),'src/alloc.rs':(r/'src/alloc.rs').read_text(),'src/profile.rs':(r/'src/profile.rs').read_text(),'src/output.rs':(r/'src/output.rs').read_text() if path.stem in encoded else oldout}
 # Grouped candidates use the same output name API; only source layout differs.
 patch=''
 for name,after in files.items():
  before=original(name)
  patch+=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/'+name if before else '/dev/null',tofile='b/'+name))
 (out/(path.stem+'.patch')).write_text(patch)
(out/'README.md').write_text('''# Archived source experiments

These patches apply to the frozen baseline commit
`d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. Each materializes one later
candidate plus common profiling/valid-ingest support. Older candidates have
not been reformatted into the final code style. The common parser/allocator
helpers are behaviorally the same; formatting and diagnostic counters evolved.
This is an algorithm replay aid, not a promise of bit-identical historical ELFs.
The raw measurements identify the actual executables used in the session.

For example, create an ignored worktree with
`git worktree add --detach experiments/replay/one-page d2dc441`, apply
`one-page.patch` there, and build with the locked release profile. Read the
experiment ledger and protocol before comparing a replay. No dataset is
contained in these patches. The original and final implementations are fully
versioned; initial instrumentation-only variants have logs but no separate
archived source snapshot.
''')
print('Archived',len(list(out.glob('*.patch'))),'candidate patches.')
