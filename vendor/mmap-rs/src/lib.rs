#![doc = include_str!("../README.md")]
#![deny(
    missing_docs,
    rustdoc::broken_intra_doc_links,
    missing_debug_implementations
)]

mod areas;
pub mod error;
mod mmap;
mod os_impl;

pub use areas::*;
pub use error::Error;
pub use mmap::*;

#[cfg(test)]
mod tests {
    #[test]
    fn reserve_none() {
        use crate::{MemoryAreas, MmapNone, MmapOptions, Protection, ShareMode};

        let mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .reserve_none()
            .unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let mapping: MmapNone = mapping.try_into().unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(!region.protection.contains(Protection::READ));
        assert!(!region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn reserve() {
        use crate::{MemoryAreas, Mmap, MmapOptions, Protection, ShareMode};

        let mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .map()
            .unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let mapping: Mmap = mapping.try_into().unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(!region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn reserve_mut() {
        use crate::{MemoryAreas, MmapMut, MmapOptions, Protection, ShareMode};

        let mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .reserve_mut()
            .unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let mut mapping: MmapMut = mapping.try_into().unwrap();

        mapping[0] = 0x42;

        assert!(mapping.as_ptr() != std::ptr::null());
        assert_eq!(mapping[0], 0x42);

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn map_none() {
        use crate::{MemoryAreas, MmapOptions, Protection, ShareMode};

        let mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .map_none()
            .unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(!region.protection.contains(Protection::READ));
        assert!(!region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn map() {
        use crate::{MemoryAreas, MmapOptions, Protection, ShareMode};

        let mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .map()
            .unwrap();

        assert!(mapping.as_ptr() != std::ptr::null());

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(!region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn map_mut() {
        use crate::{MemoryAreas, MmapOptions, Protection, ShareMode};

        let mut mapping = MmapOptions::new(MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .unwrap();

        mapping[0] = 0x42;

        assert!(mapping.as_ptr() != std::ptr::null());
        assert_eq!(mapping[0], 0x42);

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
    }

    #[test]
    fn map_file() {
        use crate::{MemoryAreas, MmapFlags, MmapOptions, Protection, ShareMode};
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();

        let mut bytes = vec![0u8; MmapOptions::page_size()];
        bytes[0] = 0x42;
        file.as_file_mut().write(&bytes).unwrap();

        let mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_flags(MmapFlags::SHARED)
                .with_file(file.as_file(), 0)
                .map()
                .unwrap()
        };

        let other_mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_flags(MmapFlags::SHARED)
                .with_file(file.as_file(), 0)
                .map()
                .unwrap()
        };

        assert_ne!(mapping.as_ptr(), std::ptr::null());
        assert_ne!(other_mapping.as_ptr(), std::ptr::null());
        assert_eq!(mapping[0], 0x42);
        assert_eq!(other_mapping[0], 0x42);

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Shared);
        assert!(region.protection.contains(Protection::READ));
        assert!(!region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
        #[cfg(not(target_os = "freebsd"))]
        assert!(region.path().is_some());
    }

    #[test]
    fn map_file_mut() {
        use crate::{MemoryAreas, MmapFlags, MmapOptions, Protection, ShareMode};
        use std::io::{Read, Seek, SeekFrom, Write};
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();

        let mut bytes = vec![0u8; MmapOptions::page_size()];
        file.as_file_mut().write(&bytes).unwrap();

        let mut mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_flags(MmapFlags::SHARED)
                .with_file(file.as_file(), 0)
                .map_mut()
                .unwrap()
        };

        let other_mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_flags(MmapFlags::SHARED)
                .with_file(file.as_file(), 0)
                .map()
                .unwrap()
        };

        assert_ne!(mapping.as_ptr(), std::ptr::null());
        assert_ne!(other_mapping.as_ptr(), std::ptr::null());
        mapping[0] = 0x42;
        assert_eq!(other_mapping[0], 0x42);
        mapping.flush(0..MmapOptions::page_size()).unwrap();
        file.as_file_mut().sync_all().unwrap();

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Shared);
        assert!(region.protection.contains(Protection::READ));
        assert!(region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
        #[cfg(not(target_os = "freebsd"))]
        assert!(region.path().is_some());

        file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        file.as_file().read(&mut bytes).unwrap();
        assert_eq!(bytes[0], 0x42);
    }

    #[test]
    fn map_file_private() {
        use crate::{MemoryAreas, MmapOptions, Protection, ShareMode};
        use std::io::{Read, Seek, SeekFrom, Write};
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();

        let mut bytes = vec![0u8; MmapOptions::page_size()];
        file.as_file_mut().write(&bytes).unwrap();

        let mut mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_file(file.as_file(), 0)
                .map_mut()
                .unwrap()
        };

        let other_mapping = unsafe {
            MmapOptions::new(MmapOptions::page_size())
                .unwrap()
                .with_file(file.as_file(), 0)
                .map()
                .unwrap()
        };

        assert_ne!(mapping.as_ptr(), std::ptr::null());
        assert_ne!(other_mapping.as_ptr(), std::ptr::null());
        mapping[0] = 0x42;
        assert_ne!(other_mapping[0], 0x42);
        mapping.flush(0..MmapOptions::page_size()).unwrap();
        file.as_file_mut().sync_all().unwrap();

        let region = MemoryAreas::query(mapping.as_ptr() as usize)
            .unwrap()
            .unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));
        #[cfg(not(target_os = "freebsd"))]
        assert!(region.path().is_some());

        file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        file.as_file().read(&mut bytes).unwrap();
        assert_ne!(bytes[0], 0x42);
    }

    #[test]
    fn reserve_and_split() {
        use crate::{MmapMut, MmapOptions};

        let mut mapping = MmapOptions::new(2 * MmapOptions::page_size())
            .unwrap()
            .reserve_mut()
            .unwrap();

        let rest = mapping.split_off(MmapOptions::page_size()).unwrap();

        assert!(mapping.as_ptr() < rest.as_ptr());

        let mut rest: MmapMut = rest.try_into().unwrap();

        rest[0] = 0x42;
        assert_eq!(rest[0], 0x42);

        assert_eq!(mapping.len(), MmapOptions::page_size());
        assert_eq!(rest.len(), MmapOptions::page_size());
        assert!(mapping.as_ptr() < rest.as_ptr());
    }

    #[test]
    fn split_off() {
        use crate::MmapOptions;

        let mut mapping = MmapOptions::new(2 * MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .unwrap();

        assert!(mapping.split_off(1).is_err());

        mapping[0] = 0x1;
        mapping[MmapOptions::page_size()] = 0x2;

        let rest = mapping.split_off(MmapOptions::page_size()).unwrap();

        assert_eq!(mapping[0], 0x1);
        assert_eq!(rest[0], 0x2);
        assert_eq!(mapping.len(), MmapOptions::page_size());
        assert_eq!(rest.len(), MmapOptions::page_size());
        assert!(mapping.as_ptr() < rest.as_ptr());
    }

    #[test]
    fn split_to() {
        use crate::MmapOptions;

        let mut mapping = MmapOptions::new(2 * MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .unwrap();

        assert!(mapping.split_to(1).is_err());

        mapping[0] = 0x1;
        mapping[MmapOptions::page_size()] = 0x2;

        let rest = mapping.split_to(MmapOptions::page_size()).unwrap();

        assert_eq!(mapping[0], 0x2);
        assert_eq!(rest[0], 0x1);
        assert_eq!(mapping.len(), MmapOptions::page_size());
        assert_eq!(rest.len(), MmapOptions::page_size());
        assert!(mapping.as_ptr() > rest.as_ptr());
    }

    #[test]
    fn split_file() {
        use crate::MmapOptions;
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();

        let mut bytes = vec![0u8; 2 * MmapOptions::page_size()];
        bytes[0] = 0x1;
        bytes[MmapOptions::page_size()] = 0x2;
        file.as_file_mut().write(&bytes).unwrap();

        let mut mapping = unsafe {
            MmapOptions::new(2 * MmapOptions::page_size())
                .unwrap()
                .with_file(file.as_file(), 0)
                .map()
                .unwrap()
        };

        assert!(mapping.split_off(1).is_err());

        let rest = mapping.split_off(MmapOptions::page_size()).unwrap();

        assert_eq!(mapping[0], 0x1);
        assert_eq!(rest[0], 0x2);
        assert_eq!(mapping.len(), MmapOptions::page_size());
        assert_eq!(rest.len(), MmapOptions::page_size());
        assert!(mapping.as_ptr() < rest.as_ptr());
    }

    #[test]
    fn split_and_merge() {
        use crate::{MemoryAreas, MmapOptions, Protection, ShareMode};

        // Allocate three pages.
        let mut left = MmapOptions::new(3 * MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .unwrap();

        // Split into left, middle and right.
        let mut middle = left.split_off(MmapOptions::page_size()).unwrap();
        let right = middle.split_off(MmapOptions::page_size()).unwrap();

        assert!(left.as_ptr() < middle.as_ptr());
        assert!(middle.as_ptr() < right.as_ptr());

        // Merging left and right should fail in either order.
        let Err((_, mut right)) = left.merge(right) else {
            panic!("expected merge to fail")
        };
        let Err((_, left)) = right.merge(left) else {
            panic!("expected merge to fail")
        };

        // Merging left and middle should succeed.
        let Err((_, mut left)) = middle.merge(left) else {
            panic!("expected merge to fail")
        };
        left.merge(middle).unwrap();

        // Merging left and right should succeed.
        let Err((_, mut left)) = right.merge(left) else {
            panic!("expected merge to fail")
        };
        left.merge(right).unwrap();

        // Ensure the size is correct.
        assert_eq!(left.size(), 3 * MmapOptions::page_size());

        let region = MemoryAreas::query(left.as_ptr() as usize).unwrap().unwrap();

        assert_eq!(region.share_mode, ShareMode::Private);
        assert!(region.protection.contains(Protection::READ));
        assert!(region.protection.contains(Protection::WRITE));
        assert!(!region.protection.contains(Protection::EXECUTE));

        // Ensure that the region we got has the same start address or a lower one.
        assert!(region.range.start <= left.start());

        // Calculate the actual size from the start address.
        let padding = left.start().saturating_sub(region.range.start);
        let size = region.range.len().saturating_sub(padding);

        // Ensure that the region is at least large enough to fully cover our memory mapping.
        assert!(size >= 3 * MmapOptions::page_size());
    }

    #[test]
    fn query_range() {
        use crate::{MemoryAreas, MmapOptions};

        // Allocate three pages.
        let mut left = MmapOptions::new(3 * MmapOptions::page_size())
            .unwrap()
            .map_mut()
            .unwrap();

        // Split into left, middle and right.
        let mut middle = left.split_off(MmapOptions::page_size()).unwrap();
        let right = middle.split_off(MmapOptions::page_size()).unwrap();

        assert!(left.as_ptr() < middle.as_ptr());
        assert!(middle.as_ptr() < right.as_ptr());

        // Drop the middle page.
        drop(middle);

        // Query the range, which should yield two memory regions.
        let start = left.as_ptr() as usize;
        let end = right.as_ptr() as usize + right.len();

        let mut areas = MemoryAreas::query_range(start..end).unwrap();

        let region = areas.next().unwrap().unwrap();
        assert_eq!(
            region.end(),
            left.as_ptr() as usize + MmapOptions::page_size()
        );
        let mut region = areas.next().unwrap().unwrap();

        if region.start() != right.as_ptr() as usize {
            region = areas.next().unwrap().unwrap();
        }

        assert_eq!(region.start(), right.as_ptr() as usize);
        assert!(areas.next().is_none());
    }
}
