use super::event::Nanoseconds;

#[cfg(target_os = "linux")]
pub(super) fn process_cpu_ns() -> Nanoseconds {
    let mut ts = nix::libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime with CLOCK_PROCESS_CPUTIME_ID is always valid.
    let ret = unsafe { nix::libc::clock_gettime(nix::libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    if ret == 0 {
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn process_cpu_ns() -> Nanoseconds {
    0
}
