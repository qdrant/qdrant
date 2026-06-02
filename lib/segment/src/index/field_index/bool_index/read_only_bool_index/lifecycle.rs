use std::path::Path;

use common::universal_io::UniversalRead;

use super::super::mutable_bool_index::{FALSES_DIRNAME, TRUES_DIRNAME};
use super::{ReadOnlyBoolIndex, ReadOnlyStorage};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

impl ReadOnlyBoolIndex {
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
    /// Returns [`Ok(None)`] when the on-disk flag directories don't exist,
    /// matching the writable counterpart's missing-index handling — the read
    /// path never creates.
    ///
    /// [1]: super::super::mutable_bool_index::MutableBoolIndex::open
    pub fn open<S: UniversalRead>(fs: &S::Fs, path: &Path) -> OperationResult<Option<Self>> {
        let Some(trues_flags) = ReadOnlyRoaringFlags::open::<S>(fs, &path.join(TRUES_DIRNAME))?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };
        let Some(falses_flags) = ReadOnlyRoaringFlags::open::<S>(fs, &path.join(FALSES_DIRNAME))?
        else {
            return Ok(None);
        };

        let indexed_count = trues_flags
            .get_bitmap()
            .union_len(falses_flags.get_bitmap()) as usize;

        Ok(Some(Self {
            _base_dir: path.to_path_buf(),
            storage: ReadOnlyStorage {
                trues_flags,
                falses_flags,
            },
            indexed_count,
        }))
    }
}
