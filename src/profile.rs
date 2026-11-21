//! Opt-in diagnostics. No per-record counters or atomics on the normal hot path.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
static ENABLED: AtomicBool = AtomicBool::new(false);
static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static BYTES: AtomicU64 = AtomicU64::new(0);
pub fn init() {
    ORIGIN.set(Instant::now()).ok();
    ENABLED.store(std::env::var_os("SS_PROFILE").is_some(), Ordering::Relaxed);
}
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}
#[inline]
pub fn allocation(n: usize) {
    if enabled() {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(n as u64, Ordering::Relaxed);
    }
}
fn cpu(clock: libc::clockid_t) -> f64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(clock, &mut t);
    }
    t.tv_sec as f64 * 1000.0 + t.tv_nsec as f64 / 1e6
}
pub struct Span {
    data: Option<(Instant, f64, f64, &'static str, usize)>,
}
impl Span {
    #[inline]
    pub fn new(stage: &'static str, id: usize) -> Self {
        Self {
            data: if enabled() {
                Some((
                    Instant::now(),
                    cpu(libc::CLOCK_THREAD_CPUTIME_ID),
                    cpu(libc::CLOCK_PROCESS_CPUTIME_ID),
                    stage,
                    id,
                ))
            } else {
                None
            },
        }
    }
    pub fn finish(self, records: usize, bytes: usize) {
        if let Some((start, thread, process, stage, id)) = self.data {
            let wall = start.elapsed().as_secs_f64() * 1000.0;
            let thread = cpu(libc::CLOCK_THREAD_CPUTIME_ID) - thread;
            let process = cpu(libc::CLOCK_PROCESS_CPUTIME_ID) - process;
            let end = ORIGIN.get().unwrap().elapsed().as_secs_f64() * 1000.0;
            eprintln!(
                "PROFILE stage={stage} id={id} end_ms={end:.3} wall_ms={wall:.3} thread_ms={thread:.3} process_ms={process:.3} records={records} bytes={bytes} worker={}",
                rayon::current_thread_index().unwrap_or(usize::MAX)
            );
        }
    }
}
pub fn summary() {
    if enabled() {
        let mut r: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe {
            libc::getrusage(libc::RUSAGE_SELF, &mut r);
        }
        eprintln!(
            "PROFILE_SUMMARY allocations={} requested_bytes={} peak_rss_kib={} minor_faults={} major_faults={} voluntary_cs={} involuntary_cs={}",
            ALLOCS.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed),
            r.ru_maxrss,
            r.ru_minflt,
            r.ru_majflt,
            r.ru_nvcsw,
            r.ru_nivcsw
        );
    }
}
