use std::path::Path;

use common::universal_io::CachedReadFs;

use super::super::mutable_bool_index::{FALSES_DIRNAME, TRUES_DIRNAME};
use super::{ReadOnlyBoolIndex, ReadOnlyStorage};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt> ReadOnlyBoolIndex<S> {
    /// Open a read-only bool index at `path`, threading every file open through
    /// the filesystem handle `fs`.
    ///
    /// `fs` is the generic filesystem object (e.g. `ReadOnlyFs<MmapFs>`): the
    /// index never touches the local filesystem directly, it opens all of its
    /// files — the `trues` and `falses` flag directories — through `fs`. Like
    /// the writable [`MutableBoolIndex::open`][1], `indexed_count`
    /// (`|trues ∪ falses|`) is derived from the two bitmaps, so `open` takes
    /// only `fs` and the directory.
    ///
    /// Returns [`Ok(None)`] only when both flag directories are absent. If
    /// exactly one of `trues` / `falses` exists, the on-disk layout is
    /// partial/corrupt: it surfaces as an error rather than a silently-missing
    /// index that would drop the persisted postings of the present half.
    ///
    /// [1]: super::super::mutable_bool_index::MutableBoolIndex::open
    pub fn open(fs: &CachedReadFs<S::Fs>, path: &Path) -> OperationResult<Option<Self>> {
        // Open both directories first so a partial layout can be distinguished
        // from a genuinely absent index, regardless of which half is missing.
        let trues_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(TRUES_DIRNAME))?;
        let falses_flags = ReadOnlyRoaringFlags::<S>::open(fs, &path.join(FALSES_DIRNAME))?;

        match (trues_flags, falses_flags) {
            // Neither directory exists: the index isn't present on disk.
            (None, None) => Ok(None),
            (Some(trues_flags), Some(falses_flags)) => {
                let indexed_count = trues_flags
                    .get_bitmap()
                    .union_len(falses_flags.get_bitmap())
                    as usize;
                let trues_count = trues_flags.count_trues();
                let falses_count = falses_flags.count_trues();

                Ok(Some(Self {
                    _base_dir: path.to_path_buf(),
                    storage: ReadOnlyStorage {
                        trues_flags,
                        falses_flags,
                    },
                    indexed_count,
                    trues_count,
                    falses_count,
                }))
            }
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
