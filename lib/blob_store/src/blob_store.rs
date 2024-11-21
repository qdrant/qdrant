use std::ops::ControlFlow;
use std::path::PathBuf;

use io::file_operations::atomic_save_json;
use lz4_flex::compress_prepend_size;
use memory::mmap_type;
use parking_lot::RwLock;

use crate::bitmask::Bitmask;
use crate::blob::Blob;
use crate::config::{StorageConfig, StorageOptions};
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, Tracker, ValuePointer};

const CONFIG_FILENAME: &str = "config.json";

pub(crate) type Result<T> = std::result::Result<T, String>;

/// Storage for values of type `V`.
///
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
#[derive(Debug)]
pub struct BlobStore<V> {
    /// Configuration of the storage.
    pub(super) config: StorageConfig,
    /// Holds mapping from `PointOffset` -> `ValuePointer`
    ///
    /// Stored in a separate file
    tracker: RwLock<Tracker>,
    /// Mapping from page_id -> mmap page
    pub(super) pages: Vec<Page>,
    /// Bitmask to represent which "blocks" of data in the pages are used and which are free.
    ///
    /// 0 is free, 1 is used.
    bitmask: RwLock<Bitmask>,
    /// Path of the directory where the storage files are stored
    base_path: PathBuf,
    _value_type: std::marker::PhantomData<V>,
}

impl<V: Blob> BlobStore<V> {
    /// LZ4 compression
    fn compress(value: &[u8]) -> Vec<u8> {
        compress_prepend_size(value)
    }

    /// LZ4 decompression
    fn decompress(value: &[u8]) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(value).unwrap()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(self.pages.len() + 1);
        // page tracker file
        for tracker_file in self.tracker.read().files() {
            paths.push(tracker_file);
        }
        // pages files
        for page_id in 0..self.next_page_id() {
            paths.push(self.page_path(page_id));
        }
        // bitmask files
        for bitmask_file in self.bitmask.read().files() {
            paths.push(bitmask_file);
        }
        // config file
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    fn next_page_id(&self) -> PageId {
        self.pages.len() as PageId
    }

    pub fn max_point_id(&self) -> PointOffset {
        self.tracker.read().pointer_count()
    }

    /// Opens an existing storage, or initializes a new one.
    ///
    /// Depends on the existence of the config file at the `base_path`.
    pub fn open_or_create(base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        let config_path = base_path.join(CONFIG_FILENAME);
        if config_path.exists() {
            Self::open(base_path)
        } else {
            // create folder if it does not exist
            std::fs::create_dir_all(&base_path)
                .map_err(|_| "Failed to create mmap sparse vector storage directory")?;
            Self::new(base_path, options)
        }
    }

    /// Initializes a new storage with a single empty page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        if !base_path.exists() {
            return Err("Base path does not exist".to_string());
        }
        if !base_path.is_dir() {
            return Err("Base path is not a directory".to_string());
        }

        let config = StorageConfig::try_from(options)?;
        let config_path = base_path.join(CONFIG_FILENAME);

        let mut storage = Self {
            tracker: RwLock::new(Tracker::new(&base_path, None)),
            pages: Default::default(),
            bitmask: RwLock::new(Bitmask::create(&base_path, config.clone())?),
            base_path,
            config: config.clone(),
            _value_type: std::marker::PhantomData,
        };

        // create first page to be covered by the bitmask
        let new_page_id = storage.next_page_id();
        let path = storage.page_path(new_page_id);
        let page = Page::new(&path, storage.config.page_size_bytes)?;
        storage.pages.push(page);

        // lastly, write config to disk to use as a signal that the storage has been created correctly
        atomic_save_json(&config_path, &config).map_err(|err| err.to_string())?;

        Ok(storage)
    }

    /// Open an existing storage at the given path
    /// Returns None if the storage does not exist
    pub fn open(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            return Err(format!("Path '{path:?}' does not exist"));
        }
        if !path.is_dir() {
            return Err(format!("Path '{path:?}' is not a directory"));
        }

        // read config file first
        let config_path = path.join(CONFIG_FILENAME);
        let config_file = std::fs::File::open(&config_path).map_err(|err| err.to_string())?;
        let config: StorageConfig =
            serde_json::from_reader(config_file).map_err(|err| err.to_string())?;

        let page_tracker = Tracker::open(&path)?;

        let bitmask = Bitmask::open(&path, config.clone())?;

        let num_pages = bitmask.infer_num_pages();

        let mut storage = Self {
            tracker: RwLock::new(page_tracker),
            config,
            pages: Vec::with_capacity(num_pages),
            bitmask: RwLock::new(bitmask),
            base_path: path.clone(),
            _value_type: std::marker::PhantomData,
        };
        // load pages
        for page_id in 0..num_pages as PageId {
            let page_path = storage.page_path(page_id);
            let page = Page::open(&page_path)?;

            storage.pages.push(page);
        }
        Ok(storage)
    }

    /// Get the path for a given page id
    pub fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    /// Read raw value from the pages. Considering that they can span more than one page.
    fn read_from_pages(
        &self,
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        mut length: u32,
    ) -> Vec<u8> {
        let mut raw_sections = Vec::with_capacity(length as usize);

        for page_id in start_page_id.. {
            let page = &self.pages[page_id as usize];
            let (raw, unread_bytes) =
                page.read_value(block_offset, length, self.config.block_size_bytes);
            raw_sections.extend(raw);

            if unread_bytes == 0 {
                break;
            }

            block_offset = 0;
            length = unread_bytes as u32;
        }

        raw_sections
    }

    /// Get the value for a given point offset
    pub fn get_value(&self, point_offset: PointOffset) -> Option<V> {
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = self.get_pointer(point_offset)?;

        let raw = self.read_from_pages(page_id, block_offset, length);
        let decompressed = Self::decompress(&raw);
        let value = V::from_bytes(&decompressed);

        Some(value)
    }

    /// Create a new page and return its id.
    /// If size is None, the page will have the default size
    ///
    /// Returns the new page id
    fn create_new_page(&mut self) -> Result<u32> {
        let new_page_id = self.next_page_id();
        let path = self.page_path(new_page_id);
        let page = Page::new(&path, self.config.page_size_bytes)?;
        self.pages.push(page);

        self.bitmask.write().cover_new_page()?;

        Ok(new_page_id)
    }

    /// Get the mapping for a given point offset
    fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.read().get(point_offset)
    }

    fn find_or_create_available_blocks(
        &mut self,
        num_blocks: u32,
    ) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        let bitmask_guard = self.bitmask.read();
        if let Some((page_id, block_offset)) = bitmask_guard.find_available_blocks(num_blocks) {
            return Ok((page_id, block_offset));
        }
        // else we need new page(s)
        let trailing_free_blocks = bitmask_guard.trailing_free_blocks();

        // release the lock before creating new pages
        drop(bitmask_guard);

        let missing_blocks = num_blocks.saturating_sub(trailing_free_blocks) as usize;

        let num_pages =
            (missing_blocks * self.config.block_size_bytes).div_ceil(self.config.page_size_bytes);
        for _ in 0..num_pages {
            self.create_new_page()?;
        }

        // At this point we are sure that we have enough free pages to allocate the blocks
        let available = self
            .bitmask
            .read()
            .find_available_blocks(num_blocks)
            .unwrap();

        Ok(available)
    }

    /// Write value into a new cell, considering that it can span more than one page
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        mut block_offset: BlockOffset,
    ) {
        let value_size = value.len();

        // Track the number of bytes that still need to be written
        let mut unwritten_tail = value_size;

        for page_id in start_page_id.. {
            let page = &mut self.pages[page_id as usize];

            let range = (value_size - unwritten_tail)..;
            unwritten_tail =
                page.write_value(block_offset, &value[range], self.config.block_size_bytes);

            if unwritten_tail == 0 {
                break;
            }

            block_offset = 0;
        }
    }

    /// Put a value in the storage.
    ///
    /// Returns true if the value existed previously and was updated, false if it was newly inserted.
    pub fn put_value(&mut self, point_offset: PointOffset, value: &V) -> Result<bool> {
        // This function needs to NOT corrupt data in case of a crash.
        //
        // Since we cannot know deterministically when a write is persisted without flushing explicitly,
        // and we don't want to flush on every write, we decided to follow this approach:
        // ┌─────────┐           ┌───────┐┌────┐┌────┐   ┌───────┐                            ┌─────┐
        // │put_value│           │Tracker││Page││Gaps│   │Bitmask│                            │flush│
        // └────┬────┘           └───┬───┘└─┬──┘└─┬──┘   └───┬───┘                            └──┬──┘
        //      │                    │      │     │          │                                   │
        //      │            Find a spot to write │          │                                   │
        //      │───────────────────────────────────────────>│                                   │
        //      │                    │      │     │          │                                   │
        //      │              Page id + offset   │          │                                   │
        //      │<───────────────────────────────────────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │    Write data to page     │     │          │                                   │
        //      │──────────────────────────>│     │          │                                   │
        //      │                    │      │     │          │                                   │
        //      │                Mark as used     │          │                                   │
        //      │───────────────────────────────────────────>│                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │Update gap│                                   │
        //      │                    │      │     │<─────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │Set pointer (in-ram)│      │     │          │                                   │
        //      │───────────────────>│      │     │          │                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │               flush               │
        //      │                    │      │     │          │<──────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │  flush   │                                   │
        //      │                    │      │     │<─────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │      flush                        │
        //      │                    │      │<───────────────────────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │   Write from memory AND flush                │
        //      │                    │<──────────────────────────────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │Mark all old as available and flush│
        //      │                    │      │     │          │<──────────────────────────────────│
        // ┌────┴────┐           ┌───┴───┐┌─┴──┐┌─┴──┐   ┌───┴───┐                            ┌──┴──┐
        // │put_value│           │Tracker││Page││Gaps│   │Bitmask│                            │flush│
        // └─────────┘           └───────┘└────┘└────┘   └───────┘                            └─────┘
        //
        // In case of crashing somewhere in the middle of this operation, the worst
        // that should happen is that we mark more cells as used than they actually are,
        // so will never reuse such space, but data will not be corrupted.

        let value_bytes = value.to_bytes();
        let comp_value = Self::compress(&value_bytes);
        let value_size = comp_value.len();

        let required_blocks = Self::blocks_for_value(value_size, self.config.block_size_bytes);
        let (start_page_id, block_offset) =
            self.find_or_create_available_blocks(required_blocks)?;

        // insert into a new cell, considering that it can span more than one page
        self.write_into_pages(&comp_value, start_page_id, block_offset);

        // mark new cell as used in the bitmask
        self.bitmask
            .write()
            .mark_blocks(start_page_id, block_offset, required_blocks, true);

        // update the pointer
        let mut tracker_guard = self.tracker.write();
        let is_update = tracker_guard.has_pointer(point_offset);
        tracker_guard.set(
            point_offset,
            ValuePointer::new(start_page_id, block_offset, value_size as u32),
        );

        // return whether it was an update or not
        Ok(is_update)
    }

    /// Delete a value from the storage
    ///
    /// Returns None if the point_offset, page, or value was not found
    ///
    /// Returns the deleted value otherwise
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Option<V> {
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = self.tracker.write().unset(point_offset)?;
        let raw = self.read_from_pages(page_id, block_offset, length);
        let decompressed = Self::decompress(&raw);
        let value = V::from_bytes(&decompressed);

        Some(value)
    }

    /// Wipe the storage, drop all pages and delete the base directory
    pub fn wipe(&mut self) {
        // clear pages
        self.pages.clear();
        // deleted base directory
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }

    /// Iterate over all the values in the storage
    pub fn iter<F>(&self, mut callback: F) -> std::io::Result<()>
    where
        F: FnMut(PointOffset, &V) -> std::io::Result<bool>,
    {
        for (point_offset, pointer) in
            self.tracker
                .read()
                .iter_pointers()
                .filter_map(|(point_offset, opt_pointer)| {
                    opt_pointer.map(|pointer| (point_offset, pointer))
                })
        {
            let ValuePointer {
                page_id,
                block_offset,
                length,
            } = pointer;

            let raw = self.read_from_pages(page_id, block_offset, length);
            let decompressed = Self::decompress(&raw);
            let value = V::from_bytes(&decompressed);
            if !callback(point_offset, &value)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Return the storage size in bytes
    pub fn get_storage_size_bytes(&self) -> usize {
        self.bitmask.read().get_storage_size_bytes()
    }

    /// Iterate over all the values in the storage, including deleted ones
    pub fn for_each_unfiltered<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(PointOffset, Option<&V>) -> ControlFlow<String, ()>,
    {
        for (point_offset, opt_pointer) in self.tracker.read().iter_pointers() {
            let value = opt_pointer.map(
                |ValuePointer {
                     page_id,
                     block_offset,
                     length,
                 }| {
                    let raw = self.read_from_pages(page_id, block_offset, length);
                    let decompressed = Self::decompress(&raw);
                    V::from_bytes(&decompressed)
                },
            );
            match callback(point_offset, value.as_ref()) {
                ControlFlow::Continue(()) => continue,
                ControlFlow::Break(message) => return Err(message),
            }
        }
        Ok(())
    }
}

impl<V> BlobStore<V> {
    /// The number of blocks needed for a given value bytes size
    fn blocks_for_value(value_size: usize, block_size: usize) -> u32 {
        value_size.div_ceil(block_size).try_into().unwrap()
    }

    /// Flush all mmaps and pending updates to disk
    pub fn flush(&self) -> std::result::Result<(), mmap_type::Error> {
        let mut bitmask_guard = self.bitmask.upgradable_read();
        bitmask_guard.flush()?;
        for page in &self.pages {
            page.flush()?;
        }
        let old_pointers = self.tracker.write().write_pending_and_flush()?;

        // update all free blocks in the bitmask
        bitmask_guard.with_upgraded(|guard| {
            for pointer in old_pointers {
                // TODO: mark in batch? so that we update the gaps less times.
                guard.mark_blocks(
                    pointer.page_id,
                    pointer.block_offset,
                    Self::blocks_for_value(pointer.length as usize, self.config.block_size_bytes),
                    false,
                );
            }
        });
        bitmask_guard.flush()?;

        Ok(())
    }
}

impl<V> Drop for BlobStore<V> {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use itertools::Itertools;
    use rand::distributions::Uniform;
    use rand::prelude::Distribution;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::blob::Blob;
    use crate::config::{
        DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS,
    };
    use crate::fixtures::{empty_storage, empty_storage_sized, random_payload, Payload, HM_FIELDS};

    #[test]
    fn test_empty_payload_storage() {
        let (_dir, storage) = empty_storage();
        let payload = storage.get_value(0);
        assert!(payload.is_none());
        assert_eq!(storage.get_storage_size_bytes(), 0);
    }

    #[test]
    fn test_put_single_empty_value() {
        let (_dir, mut storage) = empty_storage();

        // TODO: should we actually use the pages for empty values?
        let payload = Payload::default();
        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.read().mapping_len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), Payload::default());
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
    }

    #[test]
    fn test_put_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.read().mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
    }

    #[test]
    fn test_storage_files() {
        let (dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.read().mapping_len(), 1);
        let files = storage.files();
        let actual_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(
            files.len(),
            actual_files.len(),
            "The directory has {} files, but we are reporting {}\nreported: {files:?}\n actual: {actual_files:?}",
            actual_files.len(),
            files.len()
        );
        assert_eq!(files.len(), 5, "Expected 5 files, got {files:?}");
        assert_eq!(files[0].file_name().unwrap(), "tracker.dat");
        assert_eq!(files[1].file_name().unwrap(), "page_0.dat");
        assert_eq!(files[2].file_name().unwrap(), "bitmask.dat");
        assert_eq!(files[3].file_name().unwrap(), "gaps.dat");
        assert_eq!(files[4].file_name().unwrap(), "config.json");
    }

    #[test]
    fn test_put_payload() {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::rngs::SmallRng::from_entropy();

        let mut payloads = (0..100000u32)
            .map(|point_offset| (point_offset, random_payload(rng, 2)))
            .collect::<Vec<_>>();

        for (point_offset, payload) in payloads.iter() {
            storage.put_value(*point_offset, payload).unwrap();

            let stored_payload = storage.get_value(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(&stored_payload.unwrap(), payload);
        }

        // read randomly
        payloads.shuffle(rng);
        for (point_offset, payload) in payloads.iter() {
            let stored_payload = storage.get_value(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload.clone());
        }
    }

    #[test]
    fn test_delete_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert_eq!(stored_payload, Some(payload));
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);

        // delete payload
        let deleted = storage.delete_value(0);
        assert_eq!(deleted, stored_payload);
        assert_eq!(storage.pages.len(), 1);

        // get payload again
        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_none());
        storage.flush().unwrap();
        assert_eq!(storage.get_storage_size_bytes(), 0);
    }

    #[test]
    fn test_update_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.read().mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // update payload
        let mut updated_payload = Payload::default();
        updated_payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("updated".to_string()),
        );

        storage.put_value(0, &updated_payload).unwrap();
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.read().mapping_len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), updated_payload);
    }

    #[test]
    fn test_write_across_pages() {
        let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;
        let (_dir, mut storage) = empty_storage_sized(page_size);

        storage.create_new_page().unwrap();

        let value_len = 1000;

        // Value should span 8 blocks
        let value = (0..)
            .map(|i| (i % 24) as u8)
            .take(value_len)
            .collect::<Vec<_>>();

        // Let's write it near the end
        let block_offset = DEFAULT_REGION_SIZE_BLOCKS - 10;
        storage.write_into_pages(&value, 0, block_offset as u32);

        let read_value = storage.read_from_pages(0, block_offset as u32, value_len as u32);
        assert_eq!(value, read_value);
    }

    enum Operation {
        Put(PointOffset, Payload),
        Delete(PointOffset),
        Update(PointOffset, Payload),
    }

    impl Operation {
        fn random(rng: &mut impl Rng, max_point_offset: u32) -> Self {
            let point_offset = rng.gen_range(0..=max_point_offset);
            let operation = rng.gen_range(0..3);
            match operation {
                0 => {
                    let size_factor = rng.gen_range(1..10);
                    let payload = random_payload(rng, size_factor);
                    Operation::Put(point_offset, payload)
                }
                1 => Operation::Delete(point_offset),
                2 => {
                    let size_factor = rng.gen_range(1..10);
                    let payload = random_payload(rng, size_factor);
                    Operation::Update(point_offset, payload)
                }
                _ => unreachable!(),
            }
        }
    }

    #[rstest]
    fn test_behave_like_hashmap(
        #[values(1_048_576, 2_097_152, DEFAULT_PAGE_SIZE_BYTES)] page_size: usize,
    ) {
        use std::collections::HashMap;

        let (dir, mut storage) = empty_storage_sized(page_size);

        let rng = &mut rand::rngs::SmallRng::from_entropy();
        let max_point_offset = 100000u32;

        let mut model_hashmap = HashMap::new();

        let operations = (0..100000u32)
            .map(|_| Operation::random(rng, max_point_offset))
            .collect::<Vec<_>>();

        // apply operations to storage and model_hashmap
        for operation in operations {
            match operation {
                Operation::Put(point_offset, payload) => {
                    storage.put_value(point_offset, &payload).unwrap();
                    model_hashmap.insert(point_offset, payload);
                }
                Operation::Delete(point_offset) => {
                    let old1 = storage.delete_value(point_offset);
                    let old2 = model_hashmap.remove(&point_offset);
                    assert_eq!(
                        old1, old2,
                        "same deletion failed for point_offset: {point_offset} with {old1:?} vs {old2:?}",
                    );
                }
                Operation::Update(point_offset, payload) => {
                    storage.put_value(point_offset, &payload).unwrap();
                    model_hashmap.insert(point_offset, payload);
                }
            }
        }

        // asset same length
        assert_eq!(storage.tracker.read().mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value(point_offset);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(stored_payload.as_ref(), model_payload);
        }

        // flush data
        storage.flush().unwrap();

        let before_size = storage.get_storage_size_bytes();
        // drop storage
        drop(storage);

        // reopen storage
        let storage = BlobStore::<Payload>::open(dir.path().to_path_buf()).unwrap();
        // assert same size
        assert_eq!(storage.get_storage_size_bytes(), before_size);
        // assert same length
        assert_eq!(storage.tracker.read().mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value(point_offset);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(
                stored_payload.as_ref(),
                model_payload,
                "failed for point_offset: {point_offset}",
            );
        }
    }

    #[test]
    fn test_handle_huge_payload() {
        let (_dir, mut storage) = empty_storage();
        assert_eq!(storage.pages.len(), 1);

        let mut payload = Payload::default();

        let huge_payload_size = 1024 * 1024 * 50; // 50MB

        let distr = Uniform::new('a', 'z');
        let rng = rand::rngs::SmallRng::from_entropy();

        let huge_value =
            serde_json::Value::String(distr.sample_iter(rng).take(huge_payload_size).collect());
        payload.0.insert("huge".to_string(), huge_value);

        storage.put_value(0, &payload).unwrap();
        assert_eq!(storage.pages.len(), 2);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        {
            // the fitting page should be 64MB, so we should still have about 14MB of free space
            let free_blocks = storage.bitmask.read().free_blocks_for_page(1);
            let min_expected = 1024 * 1024 * 13 / DEFAULT_BLOCK_SIZE_BYTES;
            let max_expected = 1024 * 1024 * 15 / DEFAULT_BLOCK_SIZE_BYTES;
            assert!((min_expected..max_expected).contains(&free_blocks));
        }

        {
            // delete payload
            let deleted = storage.delete_value(0);
            assert!(deleted.is_some());
            assert_eq!(storage.pages.len(), 2);

            assert!(storage.get_value(0).is_none());
        }
    }

    #[test]
    fn test_storage_persistence_basic() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        {
            let mut storage = BlobStore::new(path.clone(), Default::default()).unwrap();
            storage.put_value(0, &payload).unwrap();
            assert_eq!(storage.pages.len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, 0); // first cell

            let stored_payload = storage.get_value(0);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);

            // drop storage
            drop(storage);
        }

        // reopen storage
        let storage = BlobStore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(storage.pages.len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    #[ignore = "this test is too slow for ci, and has similar coverage to the hashmap tests"]
    fn test_with_real_hm_data() {
        const EXPECTED_LEN: usize = 105_542;

        fn write_data(storage: &mut BlobStore<Payload>, init_offset: u32) -> u32 {
            let csv_path = dataset::Dataset::HMArticles
                .download()
                .expect("download should succeed");

            let csv_file = File::open(csv_path).expect("file should open");

            let mut rdr = csv::Reader::from_reader(csv_file);
            let mut point_offset = init_offset;
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, &field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        serde_json::Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage.put_value(point_offset, &payload).unwrap();
                point_offset += 1;
            }
            point_offset
        }

        fn storage_double_pass_is_consistent(
            storage: &BlobStore<Payload>,
            right_shift_offset: u32,
        ) {
            // validate storage value equality between the two writes
            let csv_path = dataset::Dataset::HMArticles
                .download()
                .expect("download should succeed");

            let csv_file = File::open(csv_path).expect("file should open");

            let mut rdr = csv::Reader::from_reader(csv_file);
            for (row_index, result) in rdr.records().enumerate() {
                let record = result.unwrap();
                // apply shift offset
                let storage_index = row_index as u32 + right_shift_offset;
                let first = storage.get_value(storage_index).unwrap();
                let second = storage
                    .get_value(storage_index + EXPECTED_LEN as u32)
                    .unwrap();
                // assert the two payloads are equal
                assert_eq!(first, second);
                // validate the payload against record
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    assert_eq!(
                        first.0.get(*field).unwrap().as_str().unwrap(),
                        record.get(i).unwrap(),
                        "failed for id {row_index} with shift {right_shift_offset} for field: {field}",
                    );
                }
            }
        }

        let (dir, mut storage) = empty_storage();
        // load data into storage
        let point_offset = write_data(&mut storage, 0);
        assert_eq!(point_offset, EXPECTED_LEN as u32);
        assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN);
        assert_eq!(storage.pages.len(), 2);

        // write the same payload a second time
        let point_offset = write_data(&mut storage, point_offset);
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN * 2);
        assert_eq!(storage.pages.len(), 4);

        // assert storage is consistent
        storage_double_pass_is_consistent(&storage, 0);

        // drop storage
        drop(storage);

        // reopen storage
        let mut storage = BlobStore::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.pages.len(), 4);
        assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN * 2);

        // assert storage is consistent after reopening
        storage_double_pass_is_consistent(&storage, 0);

        // update values shifting point offset by 1 to the right
        // loop from the end to the beginning to avoid overwriting
        let offset: u32 = 1;
        for i in (0..EXPECTED_LEN).rev() {
            let payload = storage.get_value(i as u32).unwrap();
            // move first write to the right
            storage.put_value(i as u32 + offset, &payload).unwrap();
            // move second write to the right
            storage
                .put_value(i as u32 + offset + EXPECTED_LEN as u32, &payload)
                .unwrap();
        }
        // assert storage is consistent after updating
        storage_double_pass_is_consistent(&storage, offset);

        // wipe storage manually
        assert!(dir.path().exists());
        storage.wipe();
        assert!(!dir.path().exists());
    }

    #[test]
    fn test_payload_compression() {
        let payload = random_payload(&mut rand::rngs::SmallRng::from_entropy(), 2);
        let payload_bytes = payload.to_bytes();
        let compressed = BlobStore::<Payload>::compress(&payload_bytes);
        let decompressed = BlobStore::<Payload>::decompress(&compressed);
        let decompressed_payload = <Payload as Blob>::from_bytes(&decompressed);
        assert_eq!(payload, decompressed_payload);
    }

    #[rstest]
    #[case(64)]
    #[case(128)]
    #[case(256)]
    #[case(512)]
    fn test_different_block_sizes(#[case] block_size_bytes: usize) {
        use crate::fixtures::minimal_payload;

        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let blocks_per_page = DEFAULT_PAGE_SIZE_BYTES / block_size_bytes;
        let options = StorageOptions {
            block_size_bytes: Some(block_size_bytes),
            ..Default::default()
        };
        let mut storage = BlobStore::new(dir.path().to_path_buf(), options).unwrap();

        let payload = minimal_payload();
        let last_point_id = 3 * blocks_per_page as u32;
        for point_offset in 0..=last_point_id {
            storage.put_value(point_offset, &payload).unwrap();
        }

        storage.flush().unwrap();
        println!("{last_point_id}");

        assert_eq!(storage.pages.len(), 4);
        let last_pointer = storage.get_pointer(last_point_id).unwrap();
        assert_eq!(last_pointer.block_offset, 0);
        assert_eq!(last_pointer.page_id, 3);
    }
}
