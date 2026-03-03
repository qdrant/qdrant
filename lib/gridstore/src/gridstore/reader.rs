use std::ops::ControlFlow;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::universal_io::mmap::MmapUniversal;
use common::universal_io::read_json_via;

use super::view::GridstoreView;
use crate::blob::Blob;
use crate::config::StorageConfig;
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PageId, PointOffset};
use crate::{Result, Tracker};

pub(super) const CONFIG_FILENAME: &str = "config.json";

/// Read-only storage for values of type `V`.
///
/// Holds pages and tracker directly (no locks) since it provides only read access.
/// For read-write access, use [`super::Gridstore`].
#[derive(Debug)]
pub struct GridstoreReader<V> {
    pub(super) config: StorageConfig,
    pub(super) tracker: Tracker,
    pub(super) pages: Pages<MmapUniversal<u8>>,
    pub(super) base_path: PathBuf,
    pub(super) _value_type: std::marker::PhantomData<V>,
}

impl<V: Blob> GridstoreReader<V> {
    /// Create a [`GridstoreView`] borrowing this reader's data.
    pub fn view(&self) -> GridstoreView<'_, V, MmapUniversal<u8>> {
        GridstoreView::new(&self.config, &self.tracker, &self.pages)
    }

    /// List all files belonging to this reader (tracker, pages, config).
    ///
    /// Note: does not include bitmask files. Use [`super::Gridstore::files`] for the full list.
    pub fn files(&self) -> Vec<PathBuf> {
        let num_pages = self.pages.num_pages();
        let mut paths = Vec::with_capacity(num_pages + 2);
        for tracker_file in self.tracker.files() {
            paths.push(tracker_file);
        }
        for page_id in 0..num_pages as PageId {
            paths.push(self.page_path(page_id));
        }
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.view().max_point_offset()
    }

    /// Open an existing read-only storage at the given path.
    ///
    /// Infers page count by scanning for page files on disk.
    pub fn open(base_path: PathBuf) -> Result<Self> {
        let (config, tracker) = read_config_and_tracker(&base_path)?;

        let pages = Pages::<MmapUniversal<u8>>::open(&base_path)?;

        Ok(Self {
            tracker,
            config,
            pages,
            base_path,
            _value_type: std::marker::PhantomData,
        })
    }

    pub(super) fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    pub fn get_value<const READ_SEQUENTIAL: bool>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.view()
            .get_value::<READ_SEQUENTIAL>(point_offset, hw_counter)
    }

    pub fn iter<F, E>(
        &self,
        callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
        E: From<GridstoreError>,
    {
        let control_flow = self.view().iter(0, usize::MAX, callback, hw_counter)?;

        // we set usize::MAX as the max iteration, so we should always iterate the entire thing.
        debug_assert!(matches!(control_flow, ControlFlow::Break(())));

        Ok(())
    }

    /// Return the storage size in bytes (approximate: total page capacity).
    ///
    /// For the precise used-space calculation, use [`super::Gridstore::get_storage_size_bytes`].
    pub fn get_storage_size_bytes(&self) -> usize {
        self.view().get_storage_size_bytes()
    }
}

impl<V> GridstoreReader<V> {
    /// Populate all pages and the tracker in the mmap.
    pub fn populate(&self) -> Result<()> {
        self.pages.populate()?;
        self.tracker.populate()?;
        Ok(())
    }

    /// Drop disk cache for pages.
    pub fn clear_cache(&self) -> crate::Result<()> {
        self.pages.clear_cache()?;
        Ok(())
    }
}

/// Read config and open tracker from the base path.
///
/// Shared helper used by both [`GridstoreReader::open`] and [`super::Gridstore::open`].
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
    let config: StorageConfig = read_json_via::<MmapUniversal<u8>, StorageConfig>(&config_path)?;

    let tracker = Tracker::open(base_path)?;

    Ok((config, tracker))
}
