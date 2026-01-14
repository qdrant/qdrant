use std::ops::Range;

use schnellru::{Limiter, LruMap};

#[derive(PartialEq, Hash)]
struct Key {
    /// File descriptor
    fd: u32,
    /// Offset within the file, in `page_size` units
    page: u32,
}

/// Least Recently Used (LRU) implementation for tracking sparse chunks of files being stored in a disk cache.
pub struct Lru {
    inner: LruMap<Key, (), ByDisk, ahash::RandomState>,
}

impl Lru {
    pub fn new(disk_capacity: usize, os_page_size: usize) -> Self {
        let limiter = ByDisk::new(disk_capacity, os_page_size);

        let lru = LruMap::with_hasher(limiter, ahash::RandomState::new());

        Self { inner: lru }
    }

    pub fn touch(&mut self, file_descriptor: u32, byte_range: Range<usize>) {
        let page_size = self.inner.limiter().page_size;
        let start_page = byte_range.start / page_size;
        let end_page = byte_range.end.div_ceil(page_size);

        for page in start_page..end_page {
            let key = Key {
                fd: file_descriptor,
                page: page as u32,
            };
            self.inner.get_or_insert(key, || {});
        }
    }
}

struct ByDisk {
    /// Max number of items that should be cached at any given moment
    max_items: usize,

    /// OS page size
    page_size: usize,
}

impl ByDisk {
    /// Create a new `ByDisk` limiter with the given disk capacity and OS page size.
    fn new(disk_capacity: usize, page_size: usize) -> Self {
        let max_items = disk_capacity / page_size;
        Self {
            max_items,
            page_size,
        }
    }

    /// Estimated maximum capacity of the disk in bytes.
    fn _max_capacity(&self) -> usize {
        self.max_items * self.page_size
    }
}

impl Limiter<Key, ()> for ByDisk {
    type KeyToInsert<'a> = Key;

    // u32 allows us to have 4 * 1024^3 keys.
    // This is enough to map up to 16TB worth of data, assuming 4KB pages
    type LinkType = u32;

    #[inline]
    fn is_over_the_limit(&self, length: usize) -> bool {
        self.max_items < length
    }

    #[inline]
    fn on_insert(
        &mut self,
        _length: usize,
        key: Self::KeyToInsert<'_>,
        value: (),
    ) -> Option<(Key, ())> {
        Some((key, value))
    }

    #[inline]
    fn on_replace(
        &mut self,
        _length: usize,
        _old_key: &mut Key,
        _new_key: Self::KeyToInsert<'_>,
        _old_value: &mut (),
        _new_value: &mut (),
    ) -> bool {
        true
    }

    #[inline]
    fn on_removed(&mut self, _key: &mut Key, _value: &mut ()) {
        // todo: call fallocate on the key
    }

    #[inline]
    fn on_cleared(&mut self) {
        // todo: deallocate everything
    }

    #[inline]
    fn on_grow(&mut self, _new_memory_usage: usize) -> bool {
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_memory_usage() {
        let one_gb = 1024 * 1024 * 1024;
        let disk_capacity = 10 * one_gb; // 10GB
        let page_size = 4096;
        let num_keys: usize = disk_capacity / page_size;

        let mut lru = Lru::new(disk_capacity, page_size);

        // Insert keys by touching ranges across different file descriptors
        for i in 0..num_keys {
            let fd = (i / 1_000_000) as u32; // Spread across multiple file descriptors
            let page_offset = (i % 1_000_000) * page_size;
            lru.touch(fd, page_offset..page_offset + page_size);
        }

        // Check memory usage
        let memory_usage = lru.inner.memory_usage();
        assert!(
            memory_usage < 100 * 1024 * 1024,
            "Memory usage {memory_usage} bytes exceeds 100MB limit",
        );

        // Verify we have the expected number of entries
        assert_eq!(lru.inner.len(), num_keys);
    }
}
