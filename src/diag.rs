//! Diagnostic mode (SS_DIAG=1): measure the scoring machine, then exit non-zero so
//! the dev run reports these logs. Never used for a scored submission.

use std::io::Write;
use std::path::Path;
use std::time::Instant;

use rayon::prelude::*;

fn cat(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| format!("<{e}>"))
}

#[allow(dead_code)]
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

/// Append `!k` inside the given comma-separated fields (inside quotes when quoted).
/// '!' sorts before every ID character, so appending it keeps every byte-wise
/// order between IDs (smallest name, click ties): the copies' outputs are exact.
fn tag_line(line: &[u8], cols: &[usize], k: usize, out: &mut Vec<u8>) {
    let suffix = format!("!{k}");
    let mut col = 0usize;
    let mut start = 0usize;
    let n = line.len();
    for i in 0..=n {
        if i == n || line[i] == b',' {
            let f = &line[start..i];
            // an empty field stays empty, quoted ("") or not
            if cols.contains(&col) && !f.is_empty() && f != b"\"\"" {
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
pub fn upscale(src: &Path, dst: &Path, k: usize) {
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
    // at most 20 output files per input file (refs hold 11 bits of file number)
    let per = k.div_ceil(20);
    let groups: Vec<_> = jobs.iter().filter(|j| j.3 % per == 0).cloned().collect();
    groups.par_iter().for_each(|(d, cols, f, c0)| {
        let data = std::fs::read(f).unwrap();
        let mut out = Vec::with_capacity((data.len() + data.len() / 8) * per);
        let mut wrote_header = false;
        for c in *c0..(*c0 + per).min(k) {
            let mut first = true;
            for line in data.split(|&b| b == b'\n') {
                if first {
                    if !wrote_header {
                        out.extend_from_slice(line);
                        out.push(b'\n');
                        wrote_header = true;
                    }
                    first = false;
                    continue;
                }
                if line.is_empty() {
                    continue;
                }
                tag_line(line, cols, c, &mut out);
                out.push(b'\n');
            }
        }
        let name = format!("{}-{c0:02}.csv", f.file_stem().unwrap().to_string_lossy());
        let mut w = std::fs::File::create(dst.join(d).join(name)).unwrap();
        w.write_all(&out).unwrap();
    });
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

/// One pipeline run in a fresh child process (exec, page faults and exit teardown
/// included, like the scorer). Returns a compact line: wall/internal ms, lap
/// deltas, user/sys seconds.
fn child(tag: &str, data: &Path, out: &Path, threads: usize, extra: &[(&str, &str)]) -> String {
    // a tag starting with "alt" runs the other build shipped in the image
    let exe = if tag.starts_with("alt") {
        std::path::PathBuf::from("/ssp_alt")
    } else if tag.starts_with("np") {
        std::path::PathBuf::from("/ssp_np")
    } else {
        std::env::current_exe().unwrap()
    };
    child_bin(&exe, tag, data, out, threads, extra)
}

fn child_bin(exe: &Path, tag: &str, data: &Path, out: &Path, threads: usize, extra: &[(&str, &str)]) -> String {
    let _ = std::fs::remove_dir_all(out);
    std::fs::create_dir_all(out).unwrap();
    let mut cmd = std::process::Command::new(exe);
    cmd.env_remove("SS_DIAG")
        .env("SS_TIMING", "1")
        .env("DATA_DIR", data)
        .env("OUTPUT_DIR", out)
        .env("NCPU", threads.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let t = Instant::now();
    let mut ch = cmd.spawn().unwrap();
    let pid = ch.id() as i32;
    let mut err = String::new();
    use std::io::Read;
    ch.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    let mut st = 0i32;
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::wait4(pid, &mut st, 0, &mut ru) };
    let wall = t.elapsed().as_secs_f64() * 1e3;
    let laps: Vec<f64> = err.lines().filter_map(|l| l.split_whitespace().next()?.parse::<f64>().ok()).collect();
    let total = laps.last().copied().unwrap_or(0.0);
    let mut prev = 0.0;
    let d: Vec<String> = laps
        .iter()
        .map(|&x| {
            let v = x - prev;
            prev = x;
            format!("{v:.0}")
        })
        .collect();
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 * 1e-6;
    // sub-step laps ("  links parsed: 13.1 ms") as a compact suffix
    let sub: Vec<String> = err
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            let rest = l.strip_prefix("links ")?;
            let (name, ms) = rest.split_once(':')?;
            Some(format!("{}={}", &name[..name.len().min(4)], ms.trim().trim_end_matches(" ms").split('.').next()?))
        })
        .collect();
    let sub = if sub.is_empty() { String::new() } else { format!(" L[{}]", sub.join(" ")) };
    let dg: Vec<String> = err.lines().filter_map(|l| l.strip_prefix("digest ")).map(|l| l.to_string()).collect();
    let dg = if dg.is_empty() { String::new() } else { format!(" D[{}]", dg.join(" ")) };
    let ok = if st != 0 { format!(" EXIT={st} {}", err.lines().rev().take(3).collect::<Vec<_>>().join(" / ")) } else { String::new() };
    format!(
        "{tag} {wall:.0}/{total:.0}: {} u{:.1} s{:.1} {}G{sub}{dg}{ok}",
        d.join(" "),
        tv(ru.ru_utime),
        tv(ru.ru_stime),
        ru.ru_maxrss / (1 << 20)
    )
}

/// Map every input file and read one byte per cache line with all threads.
#[allow(dead_code)]
fn read_bench(data: &Path) -> f64 {
    let mut files = Vec::new();
    for d in ["events", "clicks", "links"] {
        for e in std::fs::read_dir(data.join(d)).unwrap().flatten() {
            files.push(e.path());
        }
    }
    let t = Instant::now();
    let maps: Vec<memmap2::Mmap> = files.iter().map(|f| unsafe { memmap2::Mmap::map(&std::fs::File::open(f).unwrap()).unwrap() }).collect();
    let s: u64 = maps
        .par_iter()
        .flat_map(|m| m.par_chunks(1 << 20))
        .map(|c| c.iter().step_by(64).map(|&b| b as u64).sum::<u64>())
        .sum();
    std::hint::black_box(s);
    t.elapsed().as_secs_f64() * 1e3
}

/// Random 8-byte reads from a `mb` MiB table by all threads, each read
/// prefetched 16 ahead (like the person lookups). Returns M lookups/s.
#[allow(dead_code)]
fn lookup_bench(mb: usize, per_thread: usize) -> f64 {
    let n = (mb << 20) / 8;
    let t: Vec<u64> = (0..n as u64).into_par_iter().map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).collect();
    let mask = n.next_power_of_two() / 2 - 1; // keep inside the table
    let nt = rayon::current_num_threads();
    let st = Instant::now();
    let s: u64 = (0..nt)
        .into_par_iter()
        .map(|th| {
            let mut acc = 0u64;
            let h = |i: usize| ((i as u64 ^ ((th as u64) << 40)).wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 20) as usize & mask;
            for i in 0..per_thread {
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    std::arch::x86_64::_mm_prefetch(t.as_ptr().add(h(i + 16)) as *const i8, std::arch::x86_64::_MM_HINT_T0)
                };
                acc = acc.wrapping_add(unsafe { *t.get_unchecked(h(i)) });
            }
            acc
        })
        .sum();
    std::hint::black_box(s);
    (nt * per_thread) as f64 / st.elapsed().as_secs_f64() / 1e6
}

/// All threads: sum a 1 GiB array (read GB/s), then stream-store over it (GB/s).
#[allow(dead_code)]
fn bw_bench() -> (f64, f64) {
    let n = (1usize << 30) / 8;
    let mut v: Vec<u64> = (0..n as u64).into_par_iter().collect();
    let t = Instant::now();
    let s: u64 = v.par_chunks(1 << 16).map(|c| c.iter().fold(0u64, |a, &x| a.wrapping_add(x))).sum();
    std::hint::black_box(s);
    let rd = 1.0 / t.elapsed().as_secs_f64();
    let t = Instant::now();
    v.par_chunks_mut(1 << 16).for_each(|c| {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let z = _mm256_set1_epi64x(7);
            let p = c.as_mut_ptr() as *mut __m256i;
            for k in 0..c.len() / 4 {
                _mm256_stream_si256(p.add(k), z);
            }
            _mm_sfence();
        }
    });
    let wr = 1.0 / t.elapsed().as_secs_f64();
    (rd * 1.073_741_824, wr * 1.073_741_824)
}

#[allow(dead_code)]
fn caches() -> String {
    let mut v = Vec::new();
    for i in 0..5 {
        let b = format!("/sys/devices/system/cpu/cpu0/cache/index{i}");
        let l = cat(&format!("{b}/level"));
        if l.starts_with('<') {
            break;
        }
        v.push(format!("L{}{}={}", l.trim(), &cat(&format!("{b}/type")).trim()[..1], cat(&format!("{b}/size")).trim()));
    }
    v.join(",")
}

/// All regular files under `dir`.
fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v = Vec::new();
    let mut st = vec![dir.to_path_buf()];
    while let Some(d) = st.pop() {
        if let Ok(rd) = std::fs::read_dir(&d) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    st.push(p);
                } else {
                    v.push(p);
                }
            }
        }
    }
    v
}

/// (resident pages, total pages) of the files under `dir`, by mincore.
#[allow(dead_code)]
fn residency(dir: &Path) -> (usize, usize) {
    let (mut yes, mut all) = (0usize, 0usize);
    for p in files_under(dir) {
        let Ok(f) = std::fs::File::open(&p) else { continue };
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let Ok(m) = (unsafe { memmap2::MmapOptions::new().map(&f) }) else { continue };
        let pages = len.div_ceil(4096);
        let mut v = vec![0u8; pages];
        if unsafe { libc::mincore(m.as_ptr() as *mut libc::c_void, len, v.as_mut_ptr()) } == 0 {
            yes += v.iter().filter(|&&b| b & 1 != 0).count();
        }
        all += pages;
    }
    (yes, all)
}

/// Evict the files under `dir` from the page cache (clean pages only).
#[allow(dead_code)]
fn evict(dir: &Path) {
    use std::os::unix::io::AsRawFd;
    for p in files_under(dir) {
        if let Ok(f) = std::fs::File::open(&p) {
            unsafe {
                libc::fdatasync(f.as_raw_fd());
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
}

/// Touch one byte per page of every file under `dir` with all threads: ms.
#[allow(dead_code)]
fn touch_all(dir: &Path) -> f64 {
    let files = files_under(dir);
    let t = Instant::now();
    let s: u64 = files
        .par_iter()
        .map(|p| {
            let Ok(f) = std::fs::File::open(p) else { return 0 };
            let Ok(m) = (unsafe { memmap2::MmapOptions::new().map(&f) }) else { return 0 };
            let mut a = 0u64;
            let mut i = 0;
            while i < m.len() {
                a = a.wrapping_add(m[i] as u64);
                i += 4096;
            }
            a
        })
        .sum();
    std::hint::black_box(s);
    t.elapsed().as_secs_f64() * 1e3
}

#[allow(dead_code)]
fn mounts_for(paths: &[&str]) -> String {
    let mi = cat("/proc/self/mountinfo");
    let mut out = Vec::new();
    for want in paths {
        // the longest mount point that prefixes the path
        let mut best: Option<(usize, String)> = None;
        for l in mi.lines() {
            let f: Vec<&str> = l.split_whitespace().collect();
            if f.len() < 10 {
                continue;
            }
            let mp = f[4];
            let sep = f.iter().position(|&x| x == "-").unwrap_or(0);
            let (fstype, src) = (f.get(sep + 1).copied().unwrap_or("?"), f.get(sep + 2).copied().unwrap_or("?"));
            if want.starts_with(mp) && best.as_ref().map_or(true, |b| mp.len() >= b.0) {
                best = Some((mp.len(), format!("{want}:{fstype}({})", &src[..src.len().min(24)])));
            }
        }
        out.push(best.map(|b| b.1).unwrap_or_else(|| format!("{want}:?")));
    }
    out.join(" ")
}

#[allow(dead_code)]
fn mount_opts(path: &str) -> String {
    let mi = cat("/proc/self/mountinfo");
    let mut best: Option<(usize, String)> = None;
    for l in mi.lines() {
        let f: Vec<&str> = l.split_whitespace().collect();
        if f.len() < 10 {
            continue;
        }
        let mp = f[4];
        let sep = f.iter().position(|&x| x == "-").unwrap_or(0);
        let opts = format!("{} {}", f[5], f.get(sep + 3).copied().unwrap_or(""));
        if path.starts_with(mp) && best.as_ref().map_or(true, |b| mp.len() >= b.0) {
            best = Some((mp.len(), opts));
        }
    }
    best.map(|b| b.1).unwrap_or_default()
}

fn avail_gb(p: &Path) -> f64 {
    let c = std::ffi::CString::new(p.to_string_lossy().as_bytes()).unwrap();
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return -1.0;
    }
    st.f_bavail as f64 * st.f_frsize as f64 / 1e9
}

fn dir_gb(p: &Path) -> f64 {
    files_under(p).iter().map(|f| std::fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum::<u64>() as f64 / 1e9
}

/// ud/cd digests from a child line, as numbers
fn digests(line: &str) -> Option<(u64, u64)> {
    let d = line.split(" D[").nth(1)?;
    let mut ud = None;
    let mut cd = None;
    let w: Vec<&str> = d.split_whitespace().collect();
    for i in 0..w.len().saturating_sub(1) {
        if w[i] == "ud" {
            ud = u64::from_str_radix(w[i + 1], 16).ok();
        }
        if w[i] == "cd" {
            cd = u64::from_str_radix(w[i + 1], 16).ok();
        }
    }
    Some((ud?, cd?))
}

pub fn main(data: &Path, out: &Path, threads: usize) {
    let t0 = Instant::now();
    let _ = crate::run(data, out, false);
    let cpu = cat("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("?").replace("model name\t: ", "");
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    let big = scratch.join("big");
    let ob = scratch.join("o_big");
    let dev_gb = dir_gb(data);
    eprintln!("D33 alt=v20 np=v20s(16B records) new=v24(names off critical path + dict parquet) lb0=v24 names before parse d0=v24 plain parquet {model} | dev {dev_gb:.3} GB, scratch avail {:.1} GB, mem.max {}", avail_gb(&scratch), cat("/sys/fs/cgroup/memory.max").trim());
    eprintln!("wall/int: open links parse dedup sess write | user sys GB");
    let dg = |k: u64, line: &str, base: Option<(u64, u64)>| -> String {
        match (digests(line), base) {
            (Some((u, c)), Some((bu, bc))) => {
                let ok = u == bu.wrapping_mul(k) && c == bc.wrapping_mul(k);
                let l = line.split(" D[").next().unwrap_or(line);
                format!("{l} {}", if ok { format!("EXACT x{k}") } else { format!("MISMATCH x{k} [{line}]") })
            }
            _ => line.to_string(),
        }
    };
    let dev = child("dev", data, &ob, threads, &[("SS_DIGEST", "1")]);
    let base_d = digests(&dev);
    eprintln!("{dev}");
    let run_k = |k: usize, vars: &[(&str, &[(&str, &str)])]| {
        let need = dev_gb * k as f64 * 1.02 + 0.5;
        let av = avail_gb(&scratch);
        if av >= 0.0 && av < need {
            eprintln!("x{k}: skipped, needs {need:.1} GB, {av:.1} GB free");
            return false;
        }
        let t = Instant::now();
        upscale(data, &big, k);
        eprintln!("x{k}: {:.2} GB in {:.1}s ({} files)", dir_gb(&big), t.elapsed().as_secs_f64(), files_under(&big).len());
        for (tag, env) in vars.iter() {
            let mut e: Vec<(&str, &str)> = env.to_vec();
            // plain A/B runs time the pipeline alone; warm-ups and variants check the digest
            if *tag == "warm" || tag.contains('-') || k == 2 {
                e.push(("SS_DIGEST", "1"));
            }
            let l = child(tag, &big, &ob, threads, &e);
            eprintln!("{}", dg(k as u64, &l, base_d));
        }
        let _ = std::fs::remove_dir_all(&big);
        true
    };
    eprintln!("{}", child("alt-dev", data, &ob, threads, &[]));
    let kb: usize = std::env::var("SS_DIAG_BIG").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let km: usize = std::env::var("SS_DIAG_MID").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let v20: [(&str, &[(&str, &str)]); 24] = [
        ("warm", &[]),
        ("alt", &[]),
        ("new", &[]),
        ("lb0", &[("SS_LB", "0")]),
        ("d0", &[("SS_DICT", "0")]),
        ("np", &[]),
        ("alt", &[]),
        ("new", &[]),
        ("lb0", &[("SS_LB", "0")]),
        ("d0", &[("SS_DICT", "0")]),
        ("np", &[]),
        ("alt", &[]),
        ("new", &[]),
        ("lb0", &[("SS_LB", "0")]),
        ("d0", &[("SS_DICT", "0")]),
        ("np", &[]),
        ("alt", &[]),
        ("new", &[]),
        ("lb0", &[("SS_LB", "0")]),
        ("d0", &[("SS_DICT", "0")]),
        ("np", &[]),
        ("new-chk", &[]),
        ("np-chk", &[]),
        ("lb0-chk", &[("SS_LB", "0")]),
    ];
    run_k(km, &v20);
    let v60: [(&str, &[(&str, &str)]); 5] = [("warm", &[]), ("alt", &[]), ("new", &[]), ("alt", &[]), ("new-chk", &[])];
    if !run_k(kb, &v60) {
        run_k(kb * 2 / 3, &v60);
    }
    let _ = std::fs::remove_dir_all(&ob);
    eprintln!("done {:.1}s", t0.elapsed().as_secs_f64());
}
