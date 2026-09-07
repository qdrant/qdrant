//! Page-cache residency probing for memory reporting.
//!
//! Prefers Linux `cachestat(2)` (kernel 6.5+), and falls back to the existing
//! temporary read-only mmap + `mincore` implementation.

use std::io;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};

/// Opens `path` and returns `(disk_bytes, resident_bytes)`.
#[cfg(unix)]
pub fn probe_memory_stats(path: impl AsRef<Path>) -> io::Result<(u64, u64)> {
    let path = path.as_ref();
    let disk_bytes = fs_err::metadata(path)?.len();
    if disk_bytes == 0 {
        return Ok((0, 0));
    }

    #[cfg(target_os = "linux")]
    if CACHESTAT_SUPPORTED.load(Ordering::Relaxed) {
        match probe_via_cachestat(path, disk_bytes) {
            Ok(resident_bytes) => return Ok((disk_bytes, resident_bytes)),
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(nix::libc::ENOSYS) | Some(nix::libc::EPERM)
                ) =>
            {
                CACHESTAT_SUPPORTED.store(false, Ordering::Relaxed);
            }
            Err(err) => return Err(err),
        }
    }

    let resident_bytes = probe_via_mincore(path)?;
    Ok((disk_bytes, resident_bytes))
}

#[cfg(target_os = "linux")]
static CACHESTAT_SUPPORTED: AtomicBool = AtomicBool::new(true);

#[cfg(target_os = "linux")]
fn probe_via_cachestat(path: &Path, disk_bytes: u64) -> io::Result<u64> {
    use std::os::fd::AsRawFd;

    #[repr(C)]
    struct CachestatRange {
        off: u64,
        len: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    struct Cachestat {
        nr_cache: u64,
        nr_dirty: u64,
        nr_writeback: u64,
        nr_evicted: u64,
        nr_recently_evicted: u64,
    }

    // cachestat is syscall 451 on Qdrant's supported Linux architectures.
    // libc 0.2.177 does not expose SYS_cachestat on all of them.
    const SYS_CACHESTAT: nix::libc::c_long = 451;

    let file = fs_err::File::open(path)?;
    let range = CachestatRange {
        off: 0,
        len: disk_bytes,
    };
    let mut stats = Cachestat::default();

    // SAFETY: the two pointers refer to live repr(C) values matching the
    // cachestat kernel ABI, and `file` remains open for the syscall.
    let result = unsafe {
        nix::libc::syscall(
            SYS_CACHESTAT,
            file.as_raw_fd(),
            &range as *const CachestatRange,
            &mut stats as *mut Cachestat,
            0u32,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    let page_size = crate::mmap::advice::page_size()
        .ok_or_else(|| io::Error::other("failed to determine page size"))?
        as u64;
    Ok(stats.nr_cache.saturating_mul(page_size).min(disk_bytes))
}

#[cfg(unix)]
fn probe_via_mincore(path: &Path) -> io::Result<u64> {
    use super::{Advice, AdviceSetting, MmapFs, OpenOptions, Populate, UniversalReadFs};

    let file = MmapFs
        .open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Advice(Advice::Normal),
            },
            (),
        )
        .map_err(|err| io::Error::other(err.to_string()))?;
    file.resident_bytes()
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn cachestat_matches_mincore_on_temp_file_when_supported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.dat");
        let page_size = crate::mmap::advice::page_size().unwrap();
        let len = page_size * 64;
        let mut file = fs_err::File::create(&path).unwrap();
        file.write_all(&vec![0xAB; len]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let via_cachestat = match probe_via_cachestat(&path, len as u64) {
            Ok(bytes) => bytes,
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(nix::libc::ENOSYS) | Some(nix::libc::EPERM)
                ) =>
            {
                return;
            }
            Err(err) => panic!("unexpected cachestat error: {err}"),
        };
        let via_mincore = probe_via_mincore(&path).unwrap();

        assert!(via_cachestat > (len as u64) / 2);
        assert!(via_mincore > (len as u64) / 2);
        assert!(via_cachestat.abs_diff(via_mincore) <= page_size as u64);
    }
}
