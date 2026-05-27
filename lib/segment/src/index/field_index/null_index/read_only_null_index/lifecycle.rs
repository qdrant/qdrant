use std::path::Path;

use common::universal_io::UniversalRead;

use super::super::mutable_null_index::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME};
use super::{ReadOnlyNullIndex, ReadOnlyStorage};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> ReadOnlyNullIndex<S> {
    /// Open a read-only null index at `path`, threading every file open through
    /// the filesystem handle `fs`.
    ///
    /// `fs` is the generic filesystem object (e.g. `ReadOnlyFs<MmapFs>`): the
    /// index never touches the local filesystem directly, it opens all of its
    /// files — the `has_values` and `is_null` flag directories — through `fs`.
    /// `total_point_count` is the segment-wide point count, the same value the
    /// writable [`MutableNullIndex::open`][1] receives; it is not derivable from
    /// the index files alone.
    ///
    /// [1]: super::super::mutable_null_index::MutableNullIndex::open
    pub fn open(fs: &S::Fs, path: &Path, total_point_count: usize) -> OperationResult<Self> {
        let has_values_flags =
            ReadOnlyRoaringFlags::open(fs, &path.join(HAS_VALUES_DIRNAME), false)?;
        let is_null_flags = ReadOnlyRoaringFlags::open(fs, &path.join(IS_NULL_DIRNAME), false)?;

        Ok(Self {
            _base_dir: path.to_path_buf(),
            storage: ReadOnlyStorage {
                has_values_flags,
                is_null_flags,
            },
            total_point_count,
        })
    }
}
