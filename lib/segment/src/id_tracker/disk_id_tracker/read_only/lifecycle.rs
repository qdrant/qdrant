//! Opening and preopening (prefetch scheduling) of the read-only tracker.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::versions::ReadOnlyVersions;
use super::{DiskMoves, ReadOnlyDiskIdTracker};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::point_moves::{Hold, PointMovesMode, SlotMoves};

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
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

    /// An `Auto` populate preloads, as search reads versions per result.
    pub(super) fn open_options(populate: Populate) -> OpenOptions {
        let populate = match populate {
            Populate::Auto => Populate::PreferBackground,
            Populate::No
            | Populate::Blocking
            | Populate::PreferBackground
            | Populate::Partial(_) => populate,
        };
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    pub(super) fn deleted_open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            // Prefetch because external_ids_batch checks point_deleted() one-by-one
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of every file [`try_open`](Self::try_open)
    /// will read. Returns `false` (nothing scheduled) when the tracker is not
    /// in the on-disk format. `populate` is the placement of the per-point
    /// data, see [`open`](Self::open).
    pub fn try_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        if !DiskMappingReader::try_preopen(fs, segment_path, populate)? {
            return Ok(false);
        }

        ReadOnlyVersions::schedule_preopen(fs, segment_path, populate)?;
        fs.schedule_open(
            &deleted_path(segment_path),
            Some(Self::deleted_open_options()),
            None,
        );

        Ok(true)
    }

    /// Open a read-only disk id tracker at `segment_path`; all per-point data
    /// except the `is_uuid` bitmap stays on the backing store. A populating
    /// `populate` primes the page cache with the mapping and versions.
    ///
    /// Errors if the segment is not in the on-disk format; use
    /// [`try_open`](Self::try_open) to probe without erroring.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<Self> {
        Self::try_open(fs, segment_path, populate)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker not found in segment {}",
                segment_path.display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the segment is
    /// not in the on-disk format (`i2e` absent).
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<Option<Self>> {
        Self::try_open_with_moves(fs, fs, segment_path, populate, PointMovesMode::Ignore)
    }

    /// [`try_open`](Self::try_open) with point moves handled per `moves`. With
    /// [`PointMovesMode::Resolve`] the whole deleted set is materialized at open, the move log is
    /// read through `raw_fs` after it, and the tombstones the log names are held back, see
    /// [`SlotMoves::open`].
    pub fn try_open_with_moves(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
        moves: PointMovesMode,
    ) -> OperationResult<Option<Self>> {
        let Some(reader) = DiskMappingReader::try_open(fs, segment_path, populate)? else {
            return Ok(None);
        };

        let versions = ReadOnlyVersions::open(fs, segment_path, populate)?;

        let deleted_file = StoredBitSlice::open(
            fs,
            deleted_path(segment_path),
            Self::deleted_open_options(),
            Default::default(),
        )?;

        let moves = match moves {
            PointMovesMode::Ignore => None,
            PointMovesMode::Resolve => {
                let raw = deleted_file.read_all()?.into_owned();
                let mut moves = SlotMoves::open(raw_fs, segment_path)?;
                let mut effective = raw.clone();
                let named: Vec<_> = moves
                    .state
                    .moved_out_slots()
                    .filter(|&slot| raw.get(slot as usize).is_some_and(|bit| *bit))
                    .collect();
                for slot in named {
                    effective.set(slot as usize, false);
                    moves.held.insert(slot, Hold::Move);
                }
                Some(Box::new(DiskMoves {
                    moves,
                    raw,
                    effective,
                }))
            }
        };

        Ok(Some(Self {
            path: segment_path.to_path_buf(),
            reader,
            versions,
            deleted_file,
            deleted_full: OnceLock::new(),
            moves,
        }))
    }
}
