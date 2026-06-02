use std::path::PathBuf;

use common::bitvec::BitVec;
use common::persisted_hashmap::{Key, UniversalHashMap};
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};
use serde::{Deserialize, Serialize};

use super::MapIndexKey;
use crate::index::field_index::stored_point_to_values::StoredPointToValues;

mod lifecycle;
mod live_reload;
mod read_ops;

pub(super) const DELETED_PATH: &str = "deleted.bin";
pub(super) const HASHMAP_PATH: &str = "values_to_points.bin";
pub(super) const CONFIG_PATH: &str = "mmap_field_index_config.json";

/// Immutable map index served directly from a [`UniversalRead`] storage backend.
///
/// The storage parameter `S` defaults to [`MmapFile`], but any `UniversalRead`
/// implementation works — e.g. io_uring or disk-cache wrappers.
///
/// On-disk state (`values_to_points.bin`, `deleted.bin`, `point_to_values.*`,
/// `mmap_field_index_config.json`) is written once during [`Self::build`] and
/// not mutated afterwards: `deleted.bin` records only the points whose payload
/// was empty at build time.
///
/// Runtime deletions live in the in-memory `Storage::deleted` bitvec. They are
/// **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove_point`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct UniversalMapIndex<N: MapIndexKey + Key + ?Sized, S: UniversalRead = MmapFile> {
    pub(super) path: PathBuf,
    pub(super) storage: Storage<N, S>,
    pub(super) deleted_count: usize,
    pub(super) total_key_value_pairs: usize,
    pub(super) is_on_disk: bool,
}

pub(super) struct Storage<N: MapIndexKey + Key + ?Sized, S: UniversalRead = MmapFile> {
    pub(super) value_to_points: UniversalHashMap<N, PointOffsetType, S>,
    pub(super) point_to_values: StoredPointToValues<N, S>,
    /// In-memory deletion bitmap. Reconstructed at load time as the union of
    /// the build-time empty-payload bits read from `deleted.bin` and the
    /// segment-level deleted bitslice supplied by the id-tracker. Not persisted.
    pub(super) deleted: BitVec,
}

impl<N: MapIndexKey + Key + ?Sized, S: UniversalRead> Storage<N, S> {
    pub(super) fn ram_usage_bytes(&self) -> usize {
        let Self {
            value_to_points: _,
            point_to_values,
            deleted,
        } = self;

        // `value_to_points` is a storage-backed hashmap with no in-memory state.
        point_to_values.ram_usage_bytes() + deleted.capacity().div_ceil(u8::BITS as usize)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct UniversalMapIndexConfig {
    pub(super) total_key_value_pairs: usize,
}
