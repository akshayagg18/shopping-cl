//! Optional Linux/x86-64 statistical PC profiler, absent from normal releases.
//! Each worker gets a targeted CLOCK_THREAD_CPUTIME_ID timer. A process timer
//! could sample an idle leader instead of the worker that consumed the CPU.
//! The signal handler only reads ucontext and updates lock-free atomics.
use std::sync::{Mutex,atomic::{AtomicBool,AtomicU64,AtomicUsize,Ordering}};
const SHIFT:usize=4;
const N:usize=1<<19;
static BASE:AtomicUsize=AtomicUsize::new(0);
static ENABLED:AtomicBool=AtomicBool::new(false);
static ACTIVE:AtomicBool=AtomicBool::new(false);
static PARSE_ONLY:AtomicBool=AtomicBool::new(false);
static TICKS:[AtomicU64;N]=[const {AtomicU64::new(0)};N];
static OUTSIDE:AtomicU64=AtomicU64::new(0);
static TIMERS:Mutex<Vec<usize>>=Mutex::new(Vec::new());

extern "C" fn sample(_:libc::c_int,_:*mut libc::siginfo_t,context:*mut libc::c_void) {
    if !ACTIVE.load(Ordering::Relaxed) {return;}
    // Linux supplied this ucontext to our SA_SIGINFO handler. No allocation,
    // locks, formatting, TLS access, or non-signal-safe library calls here.
    let pc=unsafe {(*(context as *const libc::ucontext_t)).uc_mcontext.gregs[libc::REG_RIP as usize] as usize};
    let base=BASE.load(Ordering::Relaxed);
    let bin=pc.wrapping_sub(base)>>SHIFT;
    if bin<N {TICKS[bin].fetch_add(1,Ordering::Relaxed);}
    else {OUTSIDE.fetch_add(1,Ordering::Relaxed);}
}

pub fn init() {
    if std::env::var_os("SS_PC_SAMPLE").is_none() {return;}
    let exe=std::fs::read_link("/proc/self/exe").unwrap();
    let suffix=format!(" {}",exe.display());
    let maps=std::fs::read_to_string("/proc/self/maps").unwrap();
    let base=maps.lines().filter(|l|l.ends_with(&suffix)).map(|l| {
        let f:Vec<_>=l.split_whitespace().collect();
        usize::from_str_radix(f[0].split('-').next().unwrap(),16).unwrap()-usize::from_str_radix(f[2],16).unwrap()
    }).min().expect("ELF mappings");
    BASE.store(base,Ordering::Relaxed);
    let parse_only=std::env::var("SS_PC_PHASE").is_ok_and(|s|s=="parse");
    PARSE_ONLY.store(parse_only,Ordering::Relaxed);
    ACTIVE.store(!parse_only,Ordering::Relaxed);
    let mut action:libc::sigaction=unsafe {std::mem::zeroed()};
    action.sa_sigaction=sample as *const () as usize;
    action.sa_flags=libc::SA_SIGINFO|libc::SA_RESTART;
    assert_eq!(unsafe {libc::sigemptyset(&mut action.sa_mask)},0);
    assert_eq!(unsafe {libc::sigaction(libc::SIGPROF,&action,std::ptr::null_mut())},0);
    ENABLED.store(true,Ordering::Release);
    start_thread();
}

pub fn parse_phase(active:bool) {
    if PARSE_ONLY.load(Ordering::Relaxed) {ACTIVE.store(active,Ordering::Relaxed);}
}

pub fn start_thread() {
    let mut timers=TIMERS.lock().unwrap();
    if !ENABLED.load(Ordering::Acquire) {return;}
    let mut event:libc::sigevent=unsafe {std::mem::zeroed()};
    event.sigev_notify=libc::SIGEV_THREAD_ID;
    event.sigev_signo=libc::SIGPROF;
    event.sigev_notify_thread_id=unsafe {libc::syscall(libc::SYS_gettid)} as libc::c_int;
    let mut timer:libc::timer_t=unsafe {std::mem::zeroed()};
    assert_eq!(unsafe {libc::timer_create(libc::CLOCK_THREAD_CPUTIME_ID,&mut event,&mut timer)},0,"thread CPU timer");
    let spec=libc::itimerspec{it_interval:libc::timespec{tv_sec:0,tv_nsec:1_000_000},it_value:libc::timespec{tv_sec:0,tv_nsec:1_000_000}};
    assert_eq!(unsafe {libc::timer_settime(timer,0,&spec,std::ptr::null_mut())},0);
    timers.push(timer as usize);
}

pub fn finish() {
    if !ENABLED.swap(false,Ordering::AcqRel) {return;}
    for timer in TIMERS.lock().unwrap().drain(..) {assert_eq!(unsafe {libc::timer_delete(timer as libc::timer_t)},0);}
    // A queued final signal is harmless while the counter snapshot is made.
    assert_ne!(unsafe {libc::signal(libc::SIGPROF,libc::SIG_IGN)},libc::SIG_ERR);
    use std::io::Write;
    let mut file=std::io::BufWriter::new(std::fs::File::create(std::env::var_os("SS_PC_SAMPLE").unwrap()).unwrap());
    writeln!(file,"{{\"period_cpu_ns\":1000000,\"bin_bytes\":16,\"outside_executable\":{},\"samples\":[",OUTSIDE.load(Ordering::Relaxed)).unwrap();
    let mut first=true;
    for (i,counter) in TICKS.iter().enumerate() {
        let n=counter.load(Ordering::Relaxed);
        if n!=0 {
            if !first {writeln!(file,",").unwrap();} first=false;
            write!(file,"[{},{}]",i<<SHIFT,n).unwrap();
        }
    }
    writeln!(file,"]}}").unwrap();
}
