use std::mem::size_of;
use std::path::{Path, PathBuf};

pub const VERSION_MAPPING_FILE_NAME: &str = "id_tracker.versions";

pub(crate) fn version_mapping_path(base: &Path) -> PathBuf {
    base.join(VERSION_MAPPING_FILE_NAME)
}

/// Returns the required mmap filesize for a given length of a slice of type `T`.
pub(super) fn mmap_size<T>(len: usize) -> usize {
    let item_width = size_of::<T>();
    len.div_ceil(item_width) * item_width // Make it a multiple of usize-width.
}
