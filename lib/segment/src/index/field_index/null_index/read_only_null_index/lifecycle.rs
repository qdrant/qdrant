use std::path::Path;

use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::mutable_null_index::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME};
use super::{ReadOnlyNullIndex, ReadOnlyStorage};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};

impl<S: UniversalRead> ReadOnlyNullIndex<S> {
    /// Schedule background prefetch of the files [`open`](Self::open) reads, in
    /// both flag directories.
    ///
    /// Returns whether the index exists on disk. Mirrors [`Self::open`]'s
    /// absence check: only a missing *pair* of flag directories means no index,
    /// so a half-present (corrupt) layout is reported as existing here and left
    /// for `open` to reject.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        let has_values =
            ReadOnlyRoaringFlags::<S>::preopen(fs, &path.join(HAS_VALUES_DIRNAME), populate)?;
        let is_null =
            ReadOnlyRoaringFlags::<S>::preopen(fs, &path.join(IS_NULL_DIRNAME), populate)?;
        Ok(has_values || is_null)
    }

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
    /// Returns [`Ok(None)`] only when both flag directories are absent. If
    /// exactly one of `has_values` / `is_null` exists, the on-disk layout is
    /// partial/corrupt: it surfaces as an error rather than a silently-missing
    /// index that would drop the persisted postings of the present half.
    ///
    /// [1]: super::super::mutable_null_index::MutableNullIndex::open
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        total_point_count: usize,
    ) -> OperationResult<Option<Self>> {
        // Open both directories first so a partial layout can be distinguished
        // from a genuinely absent index, regardless of which half is missing.
        let has_values_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(HAS_VALUES_DIRNAME))?;
        let is_null_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(IS_NULL_DIRNAME))?;

        match (has_values_flags, is_null_flags) {
            // Neither directory exists: the index isn't present on disk.
            (None, None) => Ok(None),
            (Some(has_values_flags), Some(is_null_flags)) => Ok(Some(Self {
                _base_dir: path.to_path_buf(),
                storage: ReadOnlyStorage {
                    has_values_flags,
                    is_null_flags,
                },
                total_point_count,
            })),
            // Exactly one directory exists: partial/corrupt storage.
            (has_values, is_null) => Err(OperationError::service_error(format!(
                "inconsistent null index at {path:?}: exactly one flag directory exists \
                 ({HAS_VALUES_DIRNAME}: {}, {IS_NULL_DIRNAME}: {})",
                has_values.is_some(),
                is_null.is_some(),
            ))),
        }
    }
}
