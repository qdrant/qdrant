use std::io::Cursor;
use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, ReadRange, TypedStorage, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::immutable_id_tracker::mappings_storage::{load_mapping, mappings_path};
use crate::id_tracker::immutable_id_tracker::versions_storage::version_mapping_path;
use crate::id_tracker::point_moves::{Hold, PointMovesMode, SlotMoves};
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// No commit mark: the format is fully committed once present.
    pub fn commit_mark_path(_segment_path: &Path) -> Option<PathBuf> {
        None
    }

    /// No commit mark, so no bound: see [`Self::commit_mark_path`].
    pub fn max_committed_offset(
        _fs: &impl CachedReadFs,
        _segment_path: &Path,
    ) -> Option<PointOffsetType> {
        None
    }

    pub(super) fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of every file [`open`](Self::open) will
    /// read
    ///
    /// Returns `false` (nothing scheduled) when the segment is not in the
    /// immutable format.
    pub fn try_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<bool> {
        if !UniversalReadFs::exists(fs, &mappings_path(segment_path))? {
            return Ok(false);
        }

        let options = Self::open_options();

        fs.schedule_open(&deleted_path(segment_path), Some(options), None);
        fs.schedule_open(&version_mapping_path(segment_path), Some(options), None);
        fs.schedule_open(&mappings_path(segment_path), Some(options), None);

        Ok(true)
    }

    /// Open a read-only view over immutable ID tracker data at `segment_path`, threading every file
    /// open through `fs`. Read-only mirror of [`ImmutableIdTracker::open`]; it never writes.
    ///
    /// The `deleted` bitslice handle is kept for [`live_reload`](Self::live_reload); versions and
    /// mappings are immutable and read once into memory.
    ///
    /// [`ImmutableIdTracker::open`]: crate::id_tracker::immutable_id_tracker::ImmutableIdTracker::open
    ///
    /// Returns `Ok(None)` when the defining `id_tracker.mappings` file is absent
    /// (i.e. the segment is not in the immutable format). The probe uses
    /// `exists`, which a caching `fs` answers from its listing snapshot — a
    /// probe-by-open would consume the file's prefetched take-once handle
    /// that [`Self::open`] needs right after.
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<Self>> {
        Self::try_open_with_moves(fs, fs, segment_path, PointMovesMode::Ignore)
    }

    /// [`try_open`](Self::try_open) with point moves handled per `moves`. With
    /// [`PointMovesMode::Resolve`] the move log is read through `raw_fs` after the mask, and the
    /// tombstones it names are held back, see [`SlotMoves::open`].
    pub fn try_open_with_moves(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        moves: PointMovesMode,
    ) -> OperationResult<Option<Self>> {
        if !UniversalReadFs::exists(fs, &mappings_path(segment_path))? {
            return Ok(None);
        }
        Ok(Some(Self::open_with_moves(
            fs,
            raw_fs,
            segment_path,
            moves,
        )?))
    }

    pub fn open(fs: &impl UniversalReadFs<File = S>, segment_path: &Path) -> OperationResult<Self> {
        Self::open_with_moves(fs, fs, segment_path, PointMovesMode::Ignore)
    }

    /// [`open`](Self::open) with point moves handled per `moves`, see
    /// [`try_open_with_moves`](Self::try_open_with_moves).
    pub fn open_with_moves(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        moves: PointMovesMode,
    ) -> OperationResult<Self> {
        let options = Self::open_options();

        let deleted =
            StoredBitSlice::open(fs, deleted_path(segment_path), options, Default::default())?;
        let mut deleted_bitvec = BitVec::new();
        deleted_bitvec.extend_from_bitslice(deleted.read_all()?.as_ref());

        let moves = match moves {
            PointMovesMode::Ignore => None,
            PointMovesMode::Resolve => {
                let mut moves = SlotMoves::open(raw_fs, segment_path)?;
                let named: Vec<_> = moves
                    .state
                    .moved_out_slots()
                    .filter(|&slot| deleted_bitvec.get(slot as usize).is_some_and(|bit| *bit))
                    .collect();
                for slot in named {
                    deleted_bitvec.set(slot as usize, false);
                    moves.held.insert(slot, Hold::Move);
                }
                Some(Box::new(moves))
            }
        };

        let internal_to_version_file = TypedStorage::<S, SeqNumberType>::new(fs.open(
            version_mapping_path(segment_path),
            options,
            Default::default(),
        )?);
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);

        let mappings_file = fs.open(mappings_path(segment_path), options, Default::default())?;
        let mappings_bytes = mappings_file
            .read::<_, u8>(ReadRange::new(0, mappings_file.len::<u8>()?), Sequential)?;
        let mappings = load_mapping(Cursor::new(mappings_bytes.as_ref()), Some(deleted_bitvec))?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            deleted,
            internal_to_version,
            mappings,
            moves,
        })
    }
}
