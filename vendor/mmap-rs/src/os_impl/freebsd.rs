use crate::areas::{MemoryArea, Protection, ShareMode};
use crate::error::Error;
use crate::os_impl::unix::MmapOptions;
use crate::PageSizes;
use bitflags::bitflags;
use libc::KVME_TYPE_VNODE;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;

bitflags! {
    pub struct KvmeProtection: libc::c_int {
        const READ    = 1 << 0;
        const WRITE   = 1 << 1;
        const EXECUTE = 1 << 2;
    }
}

bitflags! {
    pub struct KvmeFlags: libc::c_int {
        const COW        = 1 << 0;
        const NEEDS_COPY = 1 << 1;
        const NOCOREDUMP = 1 << 2;
        const SUPER      = 1 << 3;
        const GROWS_UP   = 1 << 4;
        const GROWS_DOWN = 1 << 5;
        const USER_WIRED = 1 << 6;
    }
}

impl MmapOptions<'_> {
    pub fn page_sizes() -> Result<PageSizes, Error> {
        let mut sizes = 1 << Self::page_size().ilog2();

        let count = unsafe { libc::getpagesizes(std::ptr::null_mut(), 0) };

        let count = count.max(0) as usize;

        let mut page_sizes = vec![0; count];

        unsafe {
            libc::getpagesizes(page_sizes.as_mut_ptr(), page_sizes.len() as _);
        }

        for page_size in page_sizes {
            sizes |= 1 << page_size.ilog2();
        }

        Ok(PageSizes::from_bits_truncate(sizes))
    }
}

#[derive(Debug)]
pub struct MemoryAreas<B> {
    entries: Vec<libc::kinfo_vmentry>,
    index: usize,
    marker: PhantomData<B>,
    range: Option<Range<usize>>,
}

impl MemoryAreas<BufReader<File>> {
    pub fn open(pid: Option<u32>, range: Option<Range<usize>>) -> Result<Self, Error> {
        // Default to the current process if no PID was specified.
        let pid = match pid {
            Some(pid) => pid as _,
            _ => unsafe { libc::getpid() },
        };

        let mut count = 0;
        let entries_ptr = unsafe { libc::kinfo_getvmmap(pid, &mut count) };

        let entries = unsafe { core::slice::from_raw_parts(entries_ptr, count as usize) }.to_vec();

        unsafe {
            libc::free(entries_ptr as *mut core::ffi::c_void);
        }

        Ok(Self {
            entries,
            index: 0,
            marker: PhantomData,
            range,
        })
    }
}

impl<B: BufRead> Iterator for MemoryAreas<B> {
    type Item = Result<MemoryArea, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index >= self.entries.len() {
                return None;
            }

            let entry = &self.entries[self.index];
            self.index += 1;

            let flags = KvmeProtection::from_bits_truncate(entry.kve_protection);

            let mut protection = Protection::empty();

            if flags.contains(KvmeProtection::READ) {
                protection |= Protection::READ;
            }

            if flags.contains(KvmeProtection::WRITE) {
                protection |= Protection::WRITE;
            }

            if flags.contains(KvmeProtection::EXECUTE) {
                protection |= Protection::EXECUTE;
            }

            let flags = KvmeFlags::from_bits_truncate(entry.kve_flags);

            let share_mode =
                if flags.contains(KvmeFlags::COW) || flags.contains(KvmeFlags::NEEDS_COPY) {
                    ShareMode::Private
                } else if entry.kve_type == KVME_TYPE_VNODE || entry.kve_shadow_count > 1 {
                    ShareMode::Shared
                } else {
                    ShareMode::Private
                };

            let start = entry.kve_start as usize;
            let end = entry.kve_end as usize;
            let offset = entry.kve_offset;

            if let Some(ref range) = self.range {
                if end <= range.start {
                    continue;
                }

                if range.end <= start {
                    break;
                }
            }

            // Parse the path.
            let mut path: Vec<u8> = entry
                .kve_path
                .iter()
                .flatten()
                .map(|byte| *byte as u8)
                .collect();

            if let Some(end) = path.iter().position(|&c| c == 0) {
                path.truncate(end);
            }

            let path = if path.is_empty() {
                None
            } else {
                let path = match std::str::from_utf8(&path) {
                    Ok(path) => path,
                    Err(e) => return Some(Err(Error::Utf8(e))),
                };

                Some((Path::new(path).to_path_buf(), offset))
            };

            return Some(Ok(MemoryArea {
                allocation_base: start,
                range: start..end,
                protection,
                share_mode,
                path,
            }));
        }

        None
    }
}
