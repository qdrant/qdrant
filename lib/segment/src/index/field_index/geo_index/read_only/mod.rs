use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use serde_json::Value;

use super::mmap_geo_index::StoredGeoMapIndex;
use super::mutable_geo_index::read_only::ReadOnlyAppendableGeoMapIndex;
use super::read_ops::GeoMapIndexRead;
use crate::common::utils::MultiValue;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;

mod read_ops;

/// Read-only counterpart of [`GeoMapIndex`][1].
///
/// Mirrors the writable enum's shape: an `Appendable` variant for the
/// Gridstore-backed mutable format (parallel to [`MutableGeoMapIndex`][2])
/// and an `Immutable` variant for the on-disk mmap format (parallel to
/// [`ImmutableGeoMapIndex`][3] / [`StoredGeoMapIndex`][4]). The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher, no
/// write path. Query logic (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variants via
/// [`super::read_ops`].
///
/// Not yet constructible — lifecycle (open / build) lands in the follow-up
/// PR that wires up the rest of [`ReadOnlyFieldIndex`][5], matching the
/// dead-code state of `ReadOnlyNullIndex` / `ReadOnlyBoolIndex` /
/// `ReadOnlyMapIndex`.
///
/// [1]: super::GeoMapIndex
/// [2]: super::mutable_geo_index::MutableGeoMapIndex
/// [3]: super::immutable_geo_index::ImmutableGeoMapIndex
/// [4]: super::mmap_geo_index::StoredGeoMapIndex
/// [5]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
#[allow(dead_code, clippy::large_enum_variant)]
pub enum ReadOnlyGeoMapIndex<S: UniversalRead> {
    /// Loads into RAM from appendable Gridstore storage format.
    Appendable(ReadOnlyAppendableGeoMapIndex<S>),
    /// Directly reads from storage in immutable mmap format.
    Immutable(StoredGeoMapIndex<S>),
}

impl<S: UniversalRead> ReadOnlyGeoMapIndex<S> {
    /// Produce a closure that maps a point id to its indexed geo values as
    /// JSON `Value`s. Mirrors `GeoMapIndex::value_retriever`.
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            GeoMapIndexRead::get_values(self, point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}
