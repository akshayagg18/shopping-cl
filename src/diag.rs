//! Diagnostic mode (SS_DIAG=1): measure the scoring machine, then exit non-zero so
//! the dev run reports these logs. Never used for a scored submission.

use std::io::Write;
use std::path::Path;
use std::time::Instant;

use rayon::prelude::*;

fn cat(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| format!("<{e}>"))
}

fn info() {
    let cpu = cat("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("?");
    let flags = cpu.lines().find(|l| l.starts_with("flags")).unwrap_or("");
    let has = |f: &str| flags.split_whitespace().any(|x| x == f);
    eprintln!("cpu: {model}; logical in cpuinfo: {}", cpu.matches("processor\t:").count());
    eprintln!(
        "flags: avx2={} avx512f={} avx512bw={} bmi2={} pclmul={} vpclmul={}",
        has("avx2"), has("avx512f"), has("avx512bw"), has("bmi2"), has("pclmulqdq"), has("vpclmulqdq")
    );
    eprintln!("available_parallelism: {:?}", std::thread::available_parallelism());
    for f in [
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/transparent_hugepage/defrag",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/memory.max",
        "/proc/sys/kernel/osrelease",
    ] {
        eprintln!("{f}: {}", cat(f).trim());
    }
    let mi = cat("/proc/meminfo");
    for l in mi.lines().filter(|l| ["MemTotal", "MemFree", "MemAvailable", "Cached", "AnonHugePages", "Hugepagesize"].iter().any(|k| l.starts_with(k))) {
        eprintln!("meminfo {l}");
    }
    for l in cat("/proc/mounts").lines().filter(|l| ["/data", "/output", "/scratch", "/tmp", " / "].iter().any(|m| l.contains(m))) {
        eprintln!("mount {l}");
    }
    eprintln!("cpus_allowed: {}", cat("/proc/self/status").lines().find(|l| l.starts_with("Cpus_allowed_list")).unwrap_or("?"));
}

/// Append `~k` inside the given comma-separated fields (inside quotes when quoted).
fn tag_line(line: &[u8], cols: &[usize], k: usize, out: &mut Vec<u8>) {
    let suffix = format!("~{k}");
    let mut col = 0usize;
    let mut start = 0usize;
    let n = line.len();
    for i in 0..=n {
        if i == n || line[i] == b',' {
            let f = &line[start..i];
            if cols.contains(&col) && !f.is_empty() {
                if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                    out.extend_from_slice(&f[..f.len() - 1]);
                    out.extend_from_slice(suffix.as_bytes());
                    out.push(b'"');
                } else {
                    out.extend_from_slice(f);
                    out.extend_from_slice(suffix.as_bytes());
                }
            } else {
                out.extend_from_slice(f);
            }
            if i < n {
                out.push(b',');
            }
            col += 1;
            start = i + 1;
        }
    }
}

/// K renamed copies of a dataset: same window and dirt, K times the people and rows.
fn upscale(src: &Path, dst: &Path, k: usize) {
    let _ = std::fs::remove_dir_all(dst);
    for d in ["events", "clicks", "links"] {
        std::fs::create_dir_all(dst.join(d)).unwrap();
    }
    for f in ["products.csv", "campaigns.csv", "manifest.json"] {
        std::fs::copy(src.join(f), dst.join(f)).unwrap();
    }
    let mut jobs = Vec::new();
    for (d, cols) in [("events", vec![0usize, 1]), ("clicks", vec![0, 1]), ("links", vec![0, 1])] {
        let mut files: Vec<_> = std::fs::read_dir(src.join(d)).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).collect();
        files.sort();
        for f in files {
            for c in 0..k {
                jobs.push((d, cols.clone(), f.clone(), c));
            }
        }
    }
    jobs.par_iter().for_each(|(d, cols, f, c)| {
        let data = std::fs::read(f).unwrap();
        let mut out = Vec::with_capacity(data.len() + data.len() / 8);
        let mut first = true;
        for line in data.split(|&b| b == b'\n') {
            if first {
                out.extend_from_slice(line);
                first = false;
            } else {
                tag_line(line, cols, *c, &mut out);
            }
            out.push(b'\n');
        }
        // split() yields a trailing empty piece after the final '\n': drop the extra newline
        if data.ends_with(b"\n") {
            out.pop();
        }
        let name = format!("{}-{c:02}.csv", f.file_stem().unwrap().to_string_lossy());
        let mut w = std::fs::File::create(dst.join(d).join(name)).unwrap();
        w.write_all(&out).unwrap();
    });
}

fn zero_bench(huge: bool, mb: usize) -> f64 {
    unsafe {
        let len = mb << 20;
        let p = libc::mmap(std::ptr::null_mut(), len + (2 << 20), libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) as usize;
        let a = (p + (2 << 20) - 1) & !((2 << 20) - 1);
        if huge {
            libc::madvise(a as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
        } else {
            libc::madvise(a as *mut libc::c_void, len, libc::MADV_NOHUGEPAGE);
        }
        let t = Instant::now();
        // touch in parallel like the pipeline does
        (0..len / 4096).into_par_iter().with_min_len(64).for_each(|i| {
            *((a + i * 4096) as *mut u8) = 1;
        });
        let s = t.elapsed().as_secs_f64();
        libc::munmap(p as *mut libc::c_void, len + (2 << 20));
        s
    }
}

fn runs(label: &str, data: &Path, out: &Path, threads: usize, times: usize) {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    for i in 0..times {
        let _ = std::fs::remove_dir_all(out);
        std::fs::create_dir_all(out).unwrap();
        let t = Instant::now();
        let laps = pool.install(|| crate::run(data, out, false));
        let total = t.elapsed().as_secs_f64() * 1e3;
        let mut prev = 0.0;
        let parts: Vec<String> = laps
            .iter()
            .map(|(w, ms)| {
                let d = ms - prev;
                prev = *ms;
                format!("{w}={d:.0}")
            })
            .collect();
        eprintln!("RUN {label} threads={threads} #{i}: total={total:.0}ms | {}", parts.join(" "));
    }
}

fn rusage() -> String {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        format!(
            "utime={:.2}s stime={:.2}s minflt={} majflt={} maxrss={}MB",
            ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 * 1e-6,
            ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 * 1e-6,
            ru.ru_minflt,
            ru.ru_majflt,
            ru.ru_maxrss / 1024
        )
    }
}

pub fn main(data: &Path, out: &Path, threads: usize) {
    let t0 = Instant::now();
    eprintln!("==== SS_DIAG start");
    info();
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    // dev set: correctness run first (real output), then scaling
    runs("dev", data, out, threads, 3);
    for t in [1, 2, 4] {
        runs("dev", data, &scratch.join("o_dev"), t, 2);
    }
    for (huge, mb) in [(false, 1024), (true, 1024), (false, 1024), (true, 1024)] {
        eprintln!("zero-touch {mb}MB huge={huge}: {:.0} ms", zero_bench(huge, mb) * 1e3);
    }
    let big = scratch.join("big");
    let k: usize = std::env::var("SS_DIAG_K").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let t = Instant::now();
    upscale(data, &big, k);
    eprintln!("upscale x{k}: {:.0} ms ({})", t.elapsed().as_secs_f64() * 1e3, rusage());
    let ob = scratch.join("o_big");
    runs("big", &big, &ob, threads, 3);
    crate::alloc::HUGE_ON.store(false, std::sync::atomic::Ordering::Relaxed);
    runs("big-nohuge", &big, &ob, threads, 2);
    crate::alloc::HUGE_ON.store(true, std::sync::atomic::Ordering::Relaxed);
    unsafe { std::env::set_var("SS_SEQ", "1") };
    runs("big-seq", &big, &ob, threads, 1);
    unsafe { std::env::remove_var("SS_SEQ") };
    runs("big", &big, &ob, 4, 1);
    eprintln!("rusage total: {}", rusage());
    let _ = std::fs::remove_dir_all(&big);
    let _ = std::fs::remove_dir_all(&ob);
    eprintln!("==== SS_DIAG done in {:.1}s (exiting 1 on purpose so these logs are returned)", t0.elapsed().as_secs_f64());
}
