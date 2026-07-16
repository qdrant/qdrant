use std::path::PathBuf;

use common::bitvec::DeletedBitVec;
use common::universal_io::{MmapFile, SortedBlockIndex, TypedStorage, UniversalRead};

use super::Encodable;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::{OnDiskPointToValues, StoredValue};

mod lifecycle;
mod live_reload;
mod read_ops;

pub(super) const PAIRS_PATH: &str = "data.bin";
pub(super) const PAIRS_BLOCK_INDEX_PATH: &str = "data_block_index.bin";
pub(super) const DELETED_PATH: &str = "deleted.bin";
pub(super) const CONFIG_PATH: &str = "mmap_field_index_config.json";

/// Immutable numeric index served directly from a [`UniversalRead`] storage
/// backend.
///
/// On-disk state (`data.bin`, `deleted_mask.bin`, `point_to_values.*`, etc.)
/// is written once during [`Self::build`] and not mutated afterwards:
/// `deleted_mask.bin` (legacy `deleted.bin` on older segments) records only
/// the points whose payload was empty at build time.
///
/// Runtime deletions live in the in-memory `Storage::deleted` bitvec. They are
/// **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove_point`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct OnDiskNumericIndex<
    T: Encodable + Numericable + Default + StoredValue + 'static,
    S: UniversalRead = MmapFile,
> {
    pub(super) path: PathBuf,
    pub(super) storage: Storage<T, S>,
    pub(super) histogram: Histogram<T>,
    pub(super) max_values_per_point: usize,
    /// Whether the "no values" mask was read from the compact
    /// `deleted_mask.bin` or the legacy `deleted.bin`.
    pub(super) compact_deleted_mask: bool,
}

pub(in super::super) struct Storage<
    T: Encodable + Numericable + Default + StoredValue + 'static,
    S: UniversalRead = MmapFile,
> {
    pub(super) deleted: DeletedBitVec,
    // sorted pairs (id + value), sorted by value (by id if values are equal)
    pub(super) pairs: TypedStorage<S, Point<T>>,
    /// Optional in-RAM block index over `pairs`: locating a pair costs one
    /// block read instead of `O(log n)` random reads. Absent on segments
    /// built before the sidecar file was introduced.
    pub(super) pairs_block_index: Option<SortedBlockIndex<Point<T>>>,
    pub(in super::super) point_to_values: OnDiskPointToValues<T, S>,
}

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead> Storage<T, S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            deleted,
            pairs,
            pairs_block_index,
            point_to_values,
        } = self;

        deleted.ram_usage_bytes()
            + pairs.ram_usage_bytes()
            + pairs_block_index
                .as_ref()
                .map_or(0, |index| index.ram_usage_bytes())
            + point_to_values.ram_usage_bytes()
    }
}
