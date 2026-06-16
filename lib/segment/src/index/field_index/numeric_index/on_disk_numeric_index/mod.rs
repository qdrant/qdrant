use std::path::PathBuf;

use common::bitvec::BitVec;
use common::universal_io::{MmapFile, TypedStorage, UniversalRead};

use super::Encodable;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::{OnDiskPointToValues, StoredValue};

mod lifecycle;
mod live_reload;
mod read_ops;

pub(super) const PAIRS_PATH: &str = "data.bin";
pub(super) const DELETED_PATH: &str = "deleted.bin";
pub(super) const CONFIG_PATH: &str = "mmap_field_index_config.json";

/// Immutable numeric index served directly from a [`UniversalRead`] storage
/// backend.
///
/// On-disk state (`data.bin`, `deleted.bin`, `point_to_values.*`, etc.) is
/// written once during [`Self::build`] and not mutated afterwards: `deleted.bin`
/// records only the points whose payload was empty at build time.
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
    pub(super) deleted_count: usize,
    pub(super) max_values_per_point: usize,
}

pub(in super::super) struct Storage<
    T: Encodable + Numericable + Default + StoredValue + 'static,
    S: UniversalRead = MmapFile,
> {
    pub(super) deleted: BitVec,
    // sorted pairs (id + value), sorted by value (by id if values are equal)
    pub(super) pairs: TypedStorage<S, Point<T>>,
    pub(in super::super) point_to_values: OnDiskPointToValues<T, S>,
}

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead> Storage<T, S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            deleted,
            pairs,
            point_to_values,
        } = self;

        deleted.capacity().div_ceil(u8::BITS as usize)
            + pairs.ram_usage_bytes()
            + point_to_values.ram_usage_bytes()
    }
}
