use std::cmp::min;
use std::io::BufReader;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::universal_io::mmap::MmapUniversal;
use fs_err::File;
use lz4_flex::compress_prepend_size;
use parking_lot::RwLock;

use crate::Result;
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig};
use crate::error::GridstoreError;
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, Tracker, ValuePointer};

pub(super) const CONFIG_FILENAME: &str = "config.json";

#[inline]
pub(super) fn compress_lz4(value: &[u8]) -> Vec<u8> {
    compress_prepend_size(value)
}

#[inline]
pub(super) fn decompress_lz4(value: &[u8]) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(value).unwrap()
}

/// Read-only storage for values of type `V`.
///
/// Provides read access to gridstore data without write/bitmask overhead.
/// For read-write access, use [`super::Gridstore`] which wraps this type.
#[derive(Debug)]
pub struct GridstoreReader<V> {
    pub(super) config: StorageConfig,
    pub(super) tracker: Arc<RwLock<Tracker>>,
    pub(super) pages: Arc<RwLock<Vec<Page<MmapUniversal<u8>>>>>,
    pub(super) base_path: PathBuf,
    pub(super) _value_type: std::marker::PhantomData<V>,
}

impl<V: Blob> GridstoreReader<V> {
    pub(super) fn compress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => compress_lz4(&value),
        }
    }

    pub(super) fn decompress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => decompress_lz4(&value),
        }
    }

    /// List all files belonging to this reader (tracker, pages, config).
    ///
    /// Note: does not include bitmask files. Use [`super::Gridstore::files`] for the full list.
    pub fn files(&self) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(self.pages.read().len() + 1);
        for tracker_file in self.tracker.read().files() {
            paths.push(tracker_file);
        }
        for page_id in 0..self.next_page_id() {
            paths.push(self.page_path(page_id));
        }
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.base_path.join(CONFIG_FILENAME)]
    }

    pub(super) fn next_page_id(&self) -> PageId {
        self.pages.read().len() as PageId
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.tracker.read().pointer_count()
    }

    /// Open an existing read-only storage at the given path.
    ///
    /// Infers page count by scanning for page files on disk (no bitmask needed).
    pub fn open(base_path: PathBuf) -> Result<Self> {
        let (config, tracker) = Self::read_config_and_tracker(&base_path)?;
        let num_pages = Self::count_pages(&base_path);
        Self::from_parts(base_path, config, tracker, num_pages)
    }

    /// Read config and open tracker from the base path.
    pub(super) fn read_config_and_tracker(
        base_path: &std::path::Path,
    ) -> Result<(StorageConfig, Tracker)> {
        if !base_path.exists() {
            return Err(GridstoreError::service_error(format!(
                "Path '{base_path:?}' does not exist"
            )));
        }
        if !base_path.is_dir() {
            return Err(GridstoreError::service_error(format!(
                "Path '{base_path:?}' is not a directory"
            )));
        }

        let config_path = base_path.join(CONFIG_FILENAME);
        let config_file = BufReader::new(File::open(&config_path)?);
        let config: StorageConfig = serde_json::from_reader(config_file)?;

        let tracker = Tracker::open(base_path)?;

        Ok((config, tracker))
    }

    /// Construct a reader from pre-opened components and load pages.
    pub(super) fn from_parts(
        base_path: PathBuf,
        config: StorageConfig,
        tracker: Tracker,
        num_pages: usize,
    ) -> Result<Self> {
        let reader = Self {
            tracker: Arc::new(RwLock::new(tracker)),
            config,
            pages: Arc::new(RwLock::new(Vec::with_capacity(num_pages))),
            base_path,
            _value_type: std::marker::PhantomData,
        };

        let mut pages = reader.pages.write();
        for page_id in 0..num_pages as PageId {
            let page_path = reader.page_path(page_id);
            let page = Page::<MmapUniversal<u8>>::open(&page_path)?;
            pages.push(page);
        }
        drop(pages);

        Ok(reader)
    }

    fn count_pages(base_path: &std::path::Path) -> usize {
        let mut count = 0;
        while base_path.join(format!("page_{count}.dat")).exists() {
            count += 1;
        }
        count
    }

    pub(super) fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    /// Read raw value from the pages, considering that values can span more than one page.
    pub(super) fn read_from_pages<const READ_SEQUENTIAL: bool>(
        &self,
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        mut length: u32,
    ) -> Result<Vec<u8>> {
        let mut raw_sections = Vec::with_capacity(length as usize);

        let pages = self.pages.read();
        for page_id in start_page_id.. {
            let page = &pages[page_id as usize];
            let (raw, unread_bytes) = page.read_value::<READ_SEQUENTIAL>(
                block_offset,
                length,
                self.config.block_size_bytes,
            )?;

            raw_sections.extend(raw.deref());

            if unread_bytes == 0 {
                break;
            }

            block_offset = 0;
            length = unread_bytes as u32;
        }

        Ok(raw_sections)
    }

    /// Get the value for a given point offset.
    pub fn get_value<const READ_SEQUENTIAL: bool>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        let Some(pointer) = self.get_pointer(point_offset) else {
            return Ok(None);
        };

        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = pointer;

        let raw = self.read_from_pages::<READ_SEQUENTIAL>(page_id, block_offset, length)?;
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = self.decompress(raw);
        let value = V::from_bytes(&decompressed);

        Ok(Some(value))
    }

    pub(super) fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.read().get(point_offset)
    }

    /// Iterate over all the values in the storage.
    ///
    /// Stops when the callback returns `Ok(false)`.
    pub fn iter<F, E>(
        &self,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
        E: From<GridstoreError>,
    {
        const BUFFER_SIZE: usize = 128;
        let max_point_offset = self.max_point_offset();

        let mut from = 0;
        let mut buffer = Vec::with_capacity(min(BUFFER_SIZE, max_point_offset as usize));

        loop {
            buffer.clear();
            buffer.extend(
                self.tracker
                    .read()
                    .iter_pointers(from)
                    .filter_map(|(point_offset, opt_pointer)| {
                        opt_pointer.map(|pointer| (point_offset, pointer))
                    })
                    .take(BUFFER_SIZE),
            );

            if buffer.is_empty() {
                break;
            }

            from = buffer.last().unwrap().0 + 1;

            for &(point_offset, pointer) in &buffer {
                let ValuePointer {
                    page_id,
                    block_offset,
                    length,
                } = pointer;

                let raw = self.read_from_pages::<true>(page_id, block_offset, length)?;

                hw_counter.incr_delta(raw.len());

                let decompressed = self.decompress(raw);
                let value = V::from_bytes(&decompressed);
                if !callback(point_offset, value)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Return the storage size in bytes (approximate: total page capacity).
    ///
    /// For the precise used-space calculation, use [`super::Gridstore::get_storage_size_bytes`].
    pub fn get_storage_size_bytes(&self) -> usize {
        self.pages.read().len() * self.config.page_size_bytes
    }
}

impl<V> GridstoreReader<V> {
    /// Populate all pages and the tracker in the mmap.
    pub fn populate(&self) -> Result<()> {
        for page in self.pages.read().iter() {
            page.populate()?;
        }
        self.tracker.read().populate()?;
        Ok(())
    }

    /// Drop disk cache for pages.
    pub fn clear_cache(&self) -> std::io::Result<()> {
        for page in self.pages.read().iter() {
            page.clear_cache()?;
        }
        Ok(())
    }
}
