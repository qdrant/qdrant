//! Per-thread OS counters of real I/O, for query profiles.
//!
//! The hardware counters (`*_io_read_bytes`) count the bytes a request *touches* in on-disk
//! storage, whether or not they are in the page cache. These counters tell what actually came
//! from the block device: major page faults (an mmap access that had to wait for a read) and
//! bytes read from storage by the thread (page faults, readahead, `MADV_WILLNEED`, `read`).
//!
//! Sampled at the start and end of a segment scope (or a blocking phase) on the thread that
//! runs it, with one `getrusage(RUSAGE_THREAD)`. Linux only; `None` elsewhere.

/// A snapshot of the OS I/O counters of the current thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ThreadIo {
    pub major_page_faults: Option<u64>,
    pub disk_read_bytes: Option<u64>,
}

impl ThreadIo {
    /// Counters of the current thread now: one `getrusage` call, well under 1 µs.
    pub fn now() -> Self {
        cfg_select! {
            target_os = "linux" => {
                use nix::sys::resource::{UsageWho, getrusage};
                let Ok(usage) = getrusage(UsageWho::RUSAGE_THREAD) else {
                    return Self::unknown();
                };
                Self {
                    major_page_faults: u64::try_from(usage.major_page_faults()).ok(),
                    // Linux fills `ru_inblock` from the task I/O accounting: `read_bytes / 512`
                    // of `/proc/thread-self/io`, so this is the same counter without the file read
                    disk_read_bytes: task_io_accounting()
                        .then(|| u64::try_from(usage.block_reads()).ok())
                        .flatten()
                        .map(|blocks| blocks * 512),
                }
            }
            _ => Self::unknown(),
        }
    }

    pub(crate) fn unknown() -> Self {
        Self {
            major_page_faults: None,
            disk_read_bytes: None,
        }
    }

    /// Counters since `earlier`, taken on the same thread.
    pub fn since(self, earlier: Self) -> Self {
        let delta = |now: Option<u64>, before: Option<u64>| Some(now?.saturating_sub(before?));
        Self {
            major_page_faults: delta(self.major_page_faults, earlier.major_page_faults),
            disk_read_bytes: delta(self.disk_read_bytes, earlier.disk_read_bytes),
        }
    }
}

/// Whether the kernel keeps task I/O accounting (`CONFIG_TASK_IO_ACCOUNTING`). Without it,
/// `ru_inblock` is always 0, which must not read as "nothing was read from disk".
#[cfg(target_os = "linux")]
fn task_io_accounting() -> bool {
    static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVAILABLE.get_or_init(|| std::path::Path::new("/proc/self/io").exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `read_bytes` of `/proc/thread-self/io`, the source of `ru_inblock`.
    #[cfg(target_os = "linux")]
    fn proc_read_bytes() -> Option<u64> {
        fs_err::read_to_string("/proc/thread-self/io")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("read_bytes:"))?
            .trim()
            .parse()
            .ok()
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_disk_read_bytes_match_task_io_accounting() {
        let Some(before) = proc_read_bytes() else {
            return; // no task I/O accounting in this kernel
        };
        let ours = ThreadIo::now().disk_read_bytes.unwrap();
        let after = proc_read_bytes().unwrap();
        // Same counter, in 512-byte units. Bracketed: a page fault between the samples (e.g.
        // paging in our own code) can move it.
        assert!((before / 512 * 512..=after / 512 * 512).contains(&ours));
    }

    #[test]
    fn test_delta_on_this_thread() {
        let before = ThreadIo::now();
        let after = ThreadIo::now();
        let delta = after.since(before);
        if cfg!(target_os = "linux") {
            // Not asserting zero: paging in our own code on first use is a major fault too
            assert!(before.major_page_faults.is_some());
            assert!(delta.major_page_faults.is_some());
        }
        // Unknown on either side: unknown delta
        assert_eq!(after.since(ThreadIo::unknown()).disk_read_bytes, None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_major_faults_on_a_cold_mmap() {
        // A file that is not in the page cache: written, synced, and evicted
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        fs_err::write(&path, vec![7u8; 4 << 20]).unwrap();
        let file = fs_err::File::open(&path).unwrap();
        file.sync_all().unwrap();
        let evicted = nix::fcntl::posix_fadvise(
            file.file(),
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
        let mmap = unsafe { memmap2::Mmap::map(file.file()).unwrap() };

        let proc_before = proc_read_bytes();
        let before = ThreadIo::now();
        let sum: u64 = mmap.iter().step_by(4096).map(|&b| u64::from(b)).sum();
        let delta = ThreadIo::now().since(before);
        let proc_after = proc_read_bytes();
        assert_eq!(sum, 7 * 1024);
        // Eviction can be refused, or the file is on tmpfs: then the pages are simply cached
        if evicted.is_ok() && delta.major_page_faults.unwrap() > 0 {
            let bytes = delta.disk_read_bytes.unwrap();
            assert!(bytes > 0);
            if let (Some(before), Some(after)) = (proc_before, proc_after) {
                // Same counter as the task I/O accounting, in 512-byte units. Bracketed by
                // the /proc reads, so it can't be more than they saw (plus rounding).
                assert!(
                    bytes <= after - before + 512,
                    "{bytes} > {after} - {before}"
                );
            }
        }
    }
}
