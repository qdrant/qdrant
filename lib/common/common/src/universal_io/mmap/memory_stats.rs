//! Page-cache residency probing for memory reporting.
//!
//! Prefers Linux `cachestat(2)` (kernel 6.5+): no mmap, cheap for cold data.
//! Falls back to a temporary read-only mmap + `mincore` on older kernels and
//! other Unix platforms.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Files at least this large are split into parallel ranges.
const PARALLEL_PROBE_THRESHOLD: u64 = 64 * 1024 * 1024;

/// Soft upper bound on concurrent probe ranges for a single file.
const MAX_PROBE_RANGES: usize = 32;

/// Opens `path` and returns `(disk_bytes, resident_bytes)`.
///
/// Resident bytes are a point-in-time page-cache approximation.
#[cfg(unix)]
pub fn probe_memory_stats(path: impl AsRef<Path>) -> io::Result<(u64, u64)> {
    let path = path.as_ref();
    let disk_bytes = fs_err::metadata(path)?.len();
    if disk_bytes == 0 {
        return Ok((0, 0));
    }
    let resident_bytes = probe_resident_bytes(path, disk_bytes)?;
    Ok((disk_bytes, resident_bytes))
}

#[cfg(unix)]
fn probe_resident_bytes(path: &Path, disk_bytes: u64) -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        if cachestat_is_supported() {
            match probe_via_cachestat(path, disk_bytes) {
                Ok(bytes) => return Ok(bytes),
                // Unexpected errors (EACCES, ENOENT, …) propagate; ENOSYS is
                // already handled inside `cachestat_is_supported` / try path.
                Err(err) if err.raw_os_error() == Some(nix::libc::ENOSYS) => {
                    CACHESTAT_SUPPORTED.store(false, Ordering::Relaxed);
                }
                Err(err) => return Err(err),
            }
        }
    }

    probe_via_mincore(path, disk_bytes)
}

/// Sum resident bytes over page-aligned ranges, stopping only if every range fails.
fn sum_parallel_ranges<F>(disk_bytes: u64, probe_range: F) -> io::Result<u64>
where
    F: Fn(u64, u64) -> io::Result<u64> + Sync,
{
    let page_size = crate::mmap::advice::page_size()
        .ok_or_else(|| io::Error::other("failed to determine page size"))?
        as u64;
    let ranges = split_probe_ranges(disk_bytes, page_size);

    if ranges.len() == 1 {
        return probe_range(ranges[0].0, ranges[0].1);
    }

    let mut resident = 0u64;
    let mut first_err: Option<io::Error> = None;
    let mut successes = 0usize;

    std::thread::scope(|scope| {
        let probe_range = &probe_range;
        let mut handles = Vec::with_capacity(ranges.len());
        for &(offset, len) in &ranges {
            handles.push(scope.spawn(move || probe_range(offset, len)));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok(bytes)) => {
                    resident = resident.saturating_add(bytes);
                    successes += 1;
                }
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(io::Error::other("memory probe thread panicked"));
                    }
                }
            }
        }
    });

    if successes == 0 {
        Err(first_err.unwrap_or_else(|| io::Error::other("memory probe failed")))
    } else {
        Ok(resident.min(disk_bytes))
    }
}

/// Split `[0, disk_bytes)` into page-aligned ranges for parallel probing.
fn split_probe_ranges(disk_bytes: u64, page_size: u64) -> Vec<(u64, u64)> {
    if disk_bytes <= PARALLEL_PROBE_THRESHOLD || page_size == 0 {
        return vec![(0, disk_bytes)];
    }

    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(2, MAX_PROBE_RANGES);

    let mut chunk = disk_bytes.div_ceil(parallelism as u64);
    // Align chunk size up to a page boundary so ranges do not share pages.
    chunk = chunk
        .div_ceil(page_size)
        .saturating_mul(page_size)
        .max(page_size);

    let mut ranges = Vec::new();
    let mut offset = 0u64;
    while offset < disk_bytes {
        let len = (disk_bytes - offset).min(chunk);
        ranges.push((offset, len));
        offset += len;
    }
    ranges
}

/// Result of a one-shot support probe: starts optimistic, clears on ENOSYS.
#[cfg(target_os = "linux")]
static CACHESTAT_SUPPORTED: AtomicBool = AtomicBool::new(true);

#[cfg(target_os = "linux")]
fn cachestat_is_supported() -> bool {
    CACHESTAT_SUPPORTED.load(Ordering::Relaxed)
}

#[cfg(target_os = "linux")]
fn probe_via_cachestat(path: &Path, disk_bytes: u64) -> io::Result<u64> {
    let file = fs_err::File::open(path)?;
    sum_parallel_ranges(disk_bytes, |offset, len| {
        cachestat_resident_bytes(&file, offset, len)
    })
}

/// `cachestat(2)` — Linux 6.5+. Counts are in base pages (`PAGE_SIZE` units).
#[cfg(target_os = "linux")]
fn cachestat_resident_bytes(file: &fs_err::File, offset: u64, len: u64) -> io::Result<u64> {
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

    // 451 on x86_64 / aarch64 / arm / riscv64 / powerpc64 / s390x.
    // glibc headers often lack a wrapper; invoke via syscall(2).
    const SYS_CACHESTAT: nix::libc::c_long = 451;

    let range = CachestatRange {
        off: offset,
        // len == 0 means "to EOF" per cachestat(2); we always pass an explicit length.
        len,
    };
    let mut cs = Cachestat::default();

    // SAFETY: `range` / `cs` are stack-allocated repr(C) structs matching the
    // kernel ABI; `file` is open for the duration of the call.
    let ret = unsafe {
        nix::libc::syscall(
            SYS_CACHESTAT,
            file.as_raw_fd(),
            &range as *const CachestatRange,
            &mut cs as *mut Cachestat,
            0u32,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    let page_size = crate::mmap::advice::page_size()
        .ok_or_else(|| io::Error::other("failed to determine page size"))?
        as u64;
    Ok((cs.nr_cache.saturating_mul(page_size)).min(len))
}

/// Fallback: one temporary read-only mmap, then `mincore` (optionally in parallel ranges).
#[cfg(unix)]
fn probe_via_mincore(path: &Path, disk_bytes: u64) -> io::Result<u64> {
    use super::{Advice, AdviceSetting, MmapFs, OpenOptions, Populate, UniversalReadFs};

    let fs = MmapFs;
    let file = fs
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
        .map_err(|e| io::Error::other(e.to_string()))?;

    sum_parallel_ranges(disk_bytes, |offset, len| {
        file.resident_bytes_range(offset, len)
    })
}

impl super::MmapFile {
    /// Returns resident bytes in `[offset, offset+len)` via `mincore`.
    #[cfg(unix)]
    pub(super) fn resident_bytes_range(&self, offset: u64, len: u64) -> io::Result<u64> {
        if len == 0 {
            return Ok(0);
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::other("resident range overflow"))?;
        if end > self.len as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "resident range [{offset}, {end}) exceeds mapping length {}",
                    self.len
                ),
            ));
        }

        let page_size = crate::mmap::advice::page_size()
            .ok_or_else(|| io::Error::other("failed to determine page size"))?;
        // mincore requires a page-aligned address; expand to covering pages.
        let aligned_offset = (offset as usize) & !(page_size - 1);
        let aligned_end = (end as usize).div_ceil(page_size) * page_size;
        let aligned_len = aligned_end - aligned_offset;
        let num_pages = aligned_len.div_ceil(page_size);
        let mut vec = vec![0u8; num_pages];

        // SAFETY: `self.ptr` is a valid page-aligned mapping for `self.len` bytes.
        // `aligned_offset + aligned_len` stays within the mapping rounded up to a page.
        let ptr = unsafe { self.ptr.0.add(aligned_offset) };
        let ret = unsafe { nix::libc::mincore(ptr.cast(), aligned_len, vec.as_mut_ptr().cast()) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        let resident_pages = vec.iter().filter(|&&b| b & 1 != 0).count();
        let resident_bytes = (resident_pages * page_size) as u64;
        Ok(resident_bytes.min(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_ranges_single_for_small_files() {
        let ranges = split_probe_ranges(1024, 4096);
        assert_eq!(ranges, vec![(0, 1024)]);
    }

    #[test]
    fn split_ranges_page_aligned_for_large_files() {
        let page = 4096u64;
        let size = 256 * 1024 * 1024;
        let ranges = split_probe_ranges(size, page);
        assert!(ranges.len() >= 2);
        let mut covered = 0u64;
        for (i, &(off, len)) in ranges.iter().enumerate() {
            assert_eq!(off, covered);
            if i + 1 < ranges.len() {
                assert_eq!(len % page, 0, "non-final chunk must be page-aligned");
            }
            covered += len;
        }
        assert_eq!(covered, size);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cachestat_matches_mincore_on_temp_file() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.dat");
        let page_size = crate::mmap::advice::page_size().unwrap();
        let len = page_size * 64;
        let mut file = fs_err::File::create(&path).unwrap();
        file.write_all(&vec![0xABu8; len]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Probe support; skip on kernels without cachestat.
        match try_cachestat_once(&path) {
            Ok(false) => {
                eprintln!("skipping: cachestat not supported");
                return;
            }
            Ok(true) => {}
            Err(err) => panic!("unexpected cachestat probe error: {err}"),
        }

        // Touch pages into cache via a normal read.
        let bytes = fs_err::read(&path).unwrap();
        assert_eq!(bytes.len(), len);

        let via_cachestat = probe_via_cachestat(&path, len as u64).unwrap();
        let via_mincore = probe_via_mincore(&path, len as u64).unwrap();

        assert!(
            via_cachestat > (len as u64) / 2,
            "cachestat not cached: {via_cachestat} of {len}"
        );
        assert!(
            via_mincore > (len as u64) / 2,
            "mincore not cached: {via_mincore} of {len}"
        );
        let delta = via_cachestat.abs_diff(via_mincore);
        assert!(
            delta <= page_size as u64,
            "cachestat ({via_cachestat}) and mincore ({via_mincore}) diverge by {delta}"
        );
    }

    #[cfg(target_os = "linux")]
    fn try_cachestat_once(path: &Path) -> io::Result<bool> {
        let file = fs_err::File::open(path)?;
        match cachestat_resident_bytes(&file, 0, 0) {
            Ok(_) => Ok(true),
            Err(err) if err.raw_os_error() == Some(nix::libc::ENOSYS) => {
                CACHESTAT_SUPPORTED.store(false, Ordering::Relaxed);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }
}
