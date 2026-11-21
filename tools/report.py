"""Generate the submission report from retained measurements, never estimates of score."""
import collections, json, pathlib, re, statistics, subprocess
R=pathlib.Path(__file__).resolve().parents[1];D=R/'experiments/results'
def load(n):return json.loads((D/n).read_text())
def stat(rows,v,k='wall_ms'):
 x=[r[k] for r in rows if r['variant']==v];return [statistics.median(x),statistics.mean(x),statistics.stdev(x) if len(x)>1 else 0,min(x),max(x)]
def fmt(x):return ' | '.join(f'{v:,.2f}' for v in x)
large=load('final-large.json');eight=load('final-large-threads8.json');base=stat(large,'baseline');opt=stat(large,'final');io=load('io-large.json')
ready=R/'submission/READY.json'
scorer=json.loads(ready.read_text())['scorer_commit'] if ready.exists() else None
lines=['# First submission report — Shopping Sessions / Codex','',
'Validated implementation prepared on2026-11-21 UTC from pinned baseline `d2dc441e6bed67a57572a7c1f12b06dad7f163cc`. The exact scorer commit is recorded in the submission receipt after committing. Original history is preserved on a distinct optimization branch.','',
f'**Local20M result: {base[0]/1000:.3f}s → {opt[0]/1000:.3f}s median, {(1-opt[0]/base[0])*100:.1f}% less elapsed time.** With NCPU=8 on this same2-CPU-quota VM: {stat(eight,"baseline")[0]/1000:.3f}s → {stat(eight,"final")[0]/1000:.3f}s, {(1-stat(eight,"final")[0]/stat(eight,"baseline")[0])*100:.1f}% faster. These are not official scores. No sub-1s or sub-0.5s official result is claimed.','',
'## Environment and method','',
'Xeon Platinum8573C VM; 3 visible logical CPUs, 2-CPU cgroup quota,8GiB RAM. CPUs0/1 share L2; CPU 2 is separate. Final direct comparisons pin0,2, interleave forward/reverse variant order, warm each variant once, use blocking `wait4` for start-to-exit time and50ms RSS sampling. No builds or generators overlap timed runs. Official resources are8 pinned hyperthreads/24GB and were unavailable. Docker is rootless vfs without cgroup enforcement. Hardware performance counters returned EPERM.','',
'Practice baseline1.73s and rank133 were user reports, not independently reproduced official results. Current live page observed leader0.36s. The page now specifies top10 final entries retimed7 times alone; this supersedes the briefing\'s top5/5.','',
'## Repeated direct measurements','',
'All units below are milliseconds. Sample standard deviation; timings include process launch and exit.','',
'| Dataset / condition | Variant | n | Median | Mean | SD | Min | Max |','|---|---|---:|---:|---:|---:|---:|---:|']
for file,label in [('final-small.json','20K / warm'),('final-dev.json','1M / warm'),('final-medium.json','5M / warm'),('final-medium-cold.json','5M / advisory cold'),('final-large.json','20M / warm / NCPU=2'),('final-large-threads8.json','20M / warm / NCPU8')]:
 rows=load(file)
 for v in ['baseline','final']:
  lines.append(f'| {label} | {v} | {sum(r["variant"]==v for r in rows)} | {fmt(stat(rows,v))} |')
lines+=['','Cold means POSIX_FADV_DONTNEED on owned input files before every run, not a reboot or a guaranteed cold storage device. Raw filesystem input blocks/fault counts are retained. NCPU8 tests still share the VM\'s2-CPU quota. Do not scale these values linearly to official hardware.','',
'|20M process metric (median, NCPU=2) | Baseline | Final |','|---|---:|---:|']
for k,label,scale in [('user_ms','User CPU ms',1),('sys_ms','System CPU ms',1),('peak_rss_kib','Peak RSS GiB',1/1048576),('avg_rss_kib','Sampled average RSS GiB',1/1048576),('minor_faults','Minor faults',1),('major_faults','Major faults',1),('output_bytes','Output bytes',1)]:
 lines.append(f'| {label} | {stat(large,"baseline",k)[0]*scale:,.3f} | {stat(large,"final",k)[0]*scale:,.3f} |')
lines+=['',f'Peak RSS falls by {(1-stat(large,"final","peak_rss_kib")[0]/stat(large,"baseline","peak_rss_kib")[0])*100:.1f}%. Mean runtime falls by {(1-opt[1]/base[1])*100:.1f}%. CPU time is summed CPU, not elapsed wall time.','',
'## Stage profile','',
'Three additional opt-in diagnostic runs. Process CPU spans below are exclusive barrier envelopes; nested task spans overlap and are not added into elapsed time. RSS averages/maxima are sampled within the envelope; <2 samples is reported as n/a. Sampling timestamps have a small process-launch offset and50ms resolution.','',
'| Stage | Median wall ms | Process CPU ms | Avg / sampled max RSS GiB | Records | Logical bytes |','|---|---:|---:|---|---:|---:|']
pr=load('final-profile-large.json');wanted=['open','lookups','links_parse_phase','dedup_click_phase','session_phase','output'];by=collections.defaultdict(list);task=collections.defaultdict(list);summaries=[]
for run in pr:
 for l in (R/run['log']).read_text().splitlines():
  if l.startswith('PROFILE '):
   d=dict(x.split('=',1) for x in l.split()[1:]);task[d['stage']].append(d)
   if d['stage'] in wanted:
    end=float(d['end_ms']);start=end-float(d['wall_ms']);samples=[x[1] for x in run.get('rss_trace',[]) if start<=x[0]<=end] if float(d['wall_ms'])>=100 else []
    d['rss_avg']=statistics.mean(samples) if len(samples)>1 else None;d['rss_max']=max(samples) if len(samples)>1 else None;by[d['stage']].append(d)
  if l.startswith('PROFILE_SUMMARY '):summaries.append(dict(x.split('=',1) for x in l.split()[1:]))
for stage in wanted:
 ds=by[stage];m=lambda k:statistics.median(float(x[k]) for x in ds)
 av=[x['rss_avg'] for x in ds if x['rss_avg'] is not None];mx=[x['rss_max'] for x in ds if x['rss_max'] is not None];rss=f'{statistics.median(av)/1048576:.3f} / {statistics.median(mx)/1048576:.3f}' if av else 'n/a'
 b=m('bytes');b=io['output_bytes'] if stage=='output' else b
 lines.append(f'| {stage} | {m("wall_ms"):.2f} | {m("process_ms"):.2f} | {rss} | {int(m("records")):,} | {int(b):,} |')
alloc=statistics.median(int(x['allocations']) for x in summaries);abytes=statistics.median(int(x['requested_bytes']) for x in summaries)
lines+=['',f'Diagnostics count {alloc:,.0f} alloc/alloc_zeroed calls and {abytes:,.0f} requested bytes per run (median). Small reallocs bypass these counters; requested bytes are cumulative, not live memory. Open records are files, lookup records are dictionary entries, links/parse records are parseable event copies, dedup/click records combine event copies and valid clicks, session records combine surviving events and clicks, output records are user rows. Input parse bytes are raw CSV; later stages report logical representation bytes, not hardware memory traffic.','']
for stage in ['dedup','session_bucket','heavy_sort']:
 ds=task[stage];w=[float(x['wall_ms']) for x in ds];n=[int(x['records']) for x in ds]
 lines.append(f'- {stage}: task wall median {statistics.median(w):.3f}ms, max {max(w):.3f}ms; max records {max(n):,}.')
lines+=['','Whale/skew partitions exist, but their individual sorts do not dominate the measured critical path. Synchronization is not separately resolved: wall/thread-CPU gaps also include descheduling. Actual8-hyperthread hardware and60M scaling still need official validation.','',
'## IO floor estimate','',f'The20M seed2026 dataset contains **{io["csv_bytes"]:,} CSV bytes**; output contains **{io["output_bytes"]:,} bytes** (NCPU8). Full CSV-content/name digest: `{io["csv_sha256"]}`. Manifest: `{json.dumps(io["manifest"])}`.','']
read=statistics.median(x['gb_s'] for x in io['input']);write=statistics.median(x['gb_s'] for x in io['output']);rms=io['csv_bytes']/read/1e6;wms=io['output_bytes']/write/1e6
lines +=[f'Seven cached AVX2 full scans measured median {read:.3f}GB/s; seven cached writes of real output bytes measured {write:.3f}GB/s. At these observed rates, reading alone takes about {rms:.1f}ms and writing {wms:.1f}ms: {rms+wms:.1f}ms serialized, or at least {max(rms,wms):.1f}ms if fully overlapped. This is an empirical IO-only estimate on this VM, not a proven physical floor or an official-machine prediction. mmap population/page-table setup and cache behavior are included in the scan. No compression/fsync assumption is substituted for the actual pipeline.','',
'## Implemented design and correctness','',
'Lossless short ASCII encodings carry exact IDs through the pipeline. Hash fallback paths compare complete bytes; unlinked fallback IDs receive exact interned keys. Parsed events are28B instead of40B, surviving events16B instead of24B. Names are decoded/referenced only at output. Bounded persistent workers reduce aggregation states/file overhead. The prefetch FIFO is circular, and slab estimates target one2MiB mapping while overflow vectors preserve correctness.','',
'Original probabilistic identifier aliases are removed. Long refs no longer truncate at65,535 bytes or64GiB offsets; partial buffers use MaybeUninit and scatter lengths are set only after initialization. Out-of-u32 ingestion dates retain full comparison data. New BMI2 use is gated by the target feature; x86-64-v3 already required BMI2/AVX2 in the baseline. Other UTF-8 IDs remain supported, not rejected.','',
'Validation completed:','',
'-36/36 official fixtures for the frozen baseline and final implementation.','- Official seed-1 1M answer: both tables exactly equal.','- Literal Python reference on20K seed314 plus four supplied fuzz mutations; final run uses8 workers.','- Targeted forced hash/fingerprint collisions,100K encoding round trips, long UTF-8 user/event IDs, pre-1970 ingestion selection, Unicode/unlinked names, missing final LF and parser block boundaries.','-20M seed2026: both final tables exactly equal frozen baseline outputs at NCPU=2 and NCPU8.','-3 release unit tests pass.', '- Additional seeds7,99,2028 (20K events/5K clicks each) match both literal-reference tables with8 workers.','',
'The repository\'s historical separate Polars reference was absent and is not claimed as rerun; the downloaded official Polars starter is empty.60M could not be validated on this8GiB VM; integer/count bounds were audited for60M, but a full final-size run remains outstanding. The generator\'s14-day/20-category/2,000-campaign limits are preserved.','',
'## Experiments and next work','',
'See [the complete ledger](experiments/EXPERIMENTS.md), [source assessment](research.md), [initial pre-change report](experiments/INITIAL_REPORT.md) and [protocol](experiments/PROTOCOL.md). Rejected designs include recomputing user hashes after dedup, unconditional byte checks on every common lookup, more partitions, borrowed-record gathering and extra grouped-record compaction.','',
'Priorities after an official score: (1) profile on the actual8-hyperthread/24GB machine and validate60M; (2) reduce timestamp/CSV parser instructions, still the largest phase; (3) measure record/table memory locality before further redistribution changes; (4) revisit heavy-first scheduling atthe official8-worker configuration; (5) optimize Arrow construction/encoding only after measuring its share there. Sub-0.5s has not been demonstrated and needs a substantially different throughput result, not extrapolation.','',
'## Docker and submission status','']
build=D/'docker-build-result.json'
if build.exists():
 b=json.loads(build.read_text());lines.append(f'Root Dockerfile build exit={b["exit_code"]}, elapsed={b["seconds"]:.1f}s. Local proxy trust supplied as an optional BuildKit CA secret; TLS verification remains enabled and no CA enters the image. The default public build uses its normal trust roots.')
else:lines.append('Root Dockerfile validation in progress; report will be refreshed before push.')
for f in ['docker-dev-comparison.log','docker-large-comparison.log']:
 if (D/f).exists():lines.append(f'{f}: {(D/f).read_text().strip()}')
if (D/'docker-large.json').exists():
 ds=load('docker-large.json');x=[v['container_ms'] for v in ds];lines += ['', '| Docker20M, NCPU8 | n | Median ms | Mean | SD | Min | Max |', '|---|---:|---:|---:|---:|---:|---:|']
 for v in ['baseline','final']:
  x=[d['container_ms'] for d in ds if d['variant']==v]
  lines.append(f'| {v} | {len(x)} | {statistics.median(x):.2f} | {statistics.mean(x):.2f} | {statistics.stdev(x):.2f} | {min(x):.2f} | {max(x):.2f} |')
 lines += ['', 'Container timings are highly variable and not an official score estimate. Both images use the same pinned Rust1.100.0 compiler, static flags and locked dependencies; baseline source is exactly the original commit. A static affinity probe proved Docker ignored requested cpuset0,2: containers actually see CPUs0,1,2. Hence Docker absolute times must not be compared with the direct0,2-pinned runs. State start-to-exit excludes CLI create/attach overhead; CLI elapsed is retained separately. The rootless daemon cannot reproduce official resource controls. The optimized static image also passed all36 official fixtures and the1M answer;20M outputs match both frozen static Docker baseline and original direct baseline.']
lines+=['','Public repository: https://github.com/akshayagg18/shopping-cl . No generated datasets are versioned. Root Dockerfile and lockfile provide a static linux/amd64 build; runtime has no network dependency. MIT notice and dependency/AI disclosures are included.','',
'**Official submission not performed.** The browser tool failed to initialize, so no authenticated competition session was verified. No practice allowance was consumed; no official time, rank or validation receipt exists for this candidate. Submit the exact validated commit through https://www.joinbuilders.org/buildathon/tracks/shopping-sessions ; wait for actual scoring feedback. The cutoff is2026-11-22 16:00UTC (11:00AM EST).']
if scorer:
 lines[2]=lines[2].replace('The exact scorer commit is recorded in the submission receipt after committing.', f'Validated scorer commit: `{scorer}` (see `submission/READY.json`).')
from format_docs import format_text
(R/'FIRST_SUBMISSION_REPORT.md').write_text(format_text('\n'.join(lines)+'\n'))
print('Report generated from raw measurements.')
