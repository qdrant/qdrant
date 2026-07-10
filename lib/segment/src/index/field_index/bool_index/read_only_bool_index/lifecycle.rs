use std::path::Path;
use std::sync::OnceLock;

use common::universal_io::{CachedReadFs, Populate, UniversalReadFs};

use super::super::mutable_bool_index::{FALSES_DIRNAME, TRUES_DIRNAME};
use super::{ReadOnlyBoolIndex, ReadOnlyStorage};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt> ReadOnlyBoolIndex<S> {
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
        let trues = ReadOnlyRoaringFlags::<S>::preopen(fs, &path.join(TRUES_DIRNAME), populate)?;
        let falses = ReadOnlyRoaringFlags::<S>::preopen(fs, &path.join(FALSES_DIRNAME), populate)?;
        Ok(trues || falses)
    }

    /// Open a read-only bool index at `path`, threading every file open through
    /// the filesystem handle `fs`.
    ///
    /// `fs` is the generic filesystem object (e.g. `ReadOnlyFs<MmapFs>`): the
    /// index never touches the local filesystem directly, it opens all of its
    /// files — the `trues` and `falses` flag directories — through `fs`. Unlike
    /// the writable [`MutableBoolIndex::open`][1], the counts (`indexed_count`
    /// and friends) are *not* derived here: doing so would scan both flags
    /// files. They are computed on first use — see [`Self::counts`].
    ///
    /// Returns [`Ok(None)`] only when both flag directories are absent. If
    /// exactly one of `trues` / `falses` exists, the on-disk layout is
    /// partial/corrupt: it surfaces as an error rather than a silently-missing
    /// index that would drop the persisted postings of the present half.
    ///
    /// [1]: super::super::mutable_bool_index::MutableBoolIndex::open
    pub fn open(fs: &impl UniversalReadFs<File = S>, path: &Path) -> OperationResult<Option<Self>> {
        // Open both directories first so a partial layout can be distinguished
        // from a genuinely absent index, regardless of which half is missing.
        let trues_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(TRUES_DIRNAME))?;
        let falses_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(FALSES_DIRNAME))?;

        match (trues_flags, falses_flags) {
            // Neither directory exists: the index isn't present on disk.
            (None, None) => Ok(None),
            (Some(trues_flags), Some(falses_flags)) => Ok(Some(Self {
                _base_dir: path.to_path_buf(),
                storage: ReadOnlyStorage {
                    trues_flags,
                    falses_flags,
                },
                counts: OnceLock::new(),
            })),
            // Exactly one directory exists: partial/corrupt storage.
            (trues, falses) => Err(OperationError::service_error(format!(
                "inconsistent bool index at {path:?}: exactly one flag directory exists \
                 ({TRUES_DIRNAME}: {}, {FALSES_DIRNAME}: {})",
                trues.is_some(),
                falses.is_some(),
            ))),
        }
    }
}
