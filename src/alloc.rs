//! Large allocations come from 2 MiB-aligned anonymous mappings advised for
//! transparent huge pages: far fewer page faults when touching gigabytes of
//! fresh records. Small allocations go to the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

pub static HUGE_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

const BIG: usize = 1 << 20;
const HUGE: usize = 2 << 20;

#[inline]
fn round(n: usize) -> usize {
    (n + HUGE - 1) & !(HUGE - 1)
}

unsafe fn map(size: usize) -> *mut u8 {
    unsafe {
        let len = round(size) + HUGE;
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        let addr = p as usize;
        let aligned = (addr + HUGE - 1) & !(HUGE - 1);
        // trim the unaligned head and the tail
        if aligned > addr {
            libc::munmap(p, aligned - addr);
        }
        let used = round(size);
        let end = addr + len;
        if end > aligned + used {
            libc::munmap((aligned + used) as *mut libc::c_void, end - aligned - used);
        }
        if HUGE_ON.load(std::sync::atomic::Ordering::Relaxed) {
            libc::madvise(aligned as *mut libc::c_void, used, libc::MADV_HUGEPAGE);
        }
        aligned as *mut u8
    }
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        unsafe { if l.size() >= BIG && l.align() <= HUGE { map(l.size()) } else { System.alloc(l) } }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        // fresh anonymous mappings are already zero
        unsafe { if l.size() >= BIG && l.align() <= HUGE { map(l.size()) } else { System.alloc_zeroed(l) } }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe {
            if l.size() >= BIG && l.align() <= HUGE {
                libc::munmap(p as *mut libc::c_void, round(l.size()));
            } else {
                System.dealloc(p, l)
            }
        }
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        unsafe {
            let old_big = l.size() >= BIG && l.align() <= HUGE;
            let new_big = new >= BIG && l.align() <= HUGE;
            if !old_big && !new_big {
                return System.realloc(p, l, new);
            }
            if old_big && new_big && round(new) == round(l.size()) {
                return p;
            }
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
                self.dealloc(p, l);
            }
            q
        }
    }
}
