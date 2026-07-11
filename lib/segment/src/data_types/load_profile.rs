//! Request-specific load profile for read-only segment opens.
//!
//! A long-lived deployment loads a segment once and serves arbitrary requests from it, so
//! every component is brought into memory according to the persisted segment config. A
//! read-only follower opened for one known request (the serverless cold-start path) can do
//! better: components the request will never touch don't have to be warmed at all.
//!
//! [`LoadProfile`] captures that knowledge. It is derived from the request *before* the
//! shard is opened and threaded down to the per-component `preopen`/`open` calls, where it
//! overrides the config-derived [`Populate`] decision. The profile only ever demotes: a
//! component it keeps "warm" loads exactly as the persisted config says, a component it
//! excludes is parked cold and read lazily on first use. It never disables a component —
//! every request the segment can serve still works, just colder.
//!
//! The per-component placement methods are all defined here, in one place, so the memory
//! placement of a whole segment under a profile can be reviewed without chasing the
//! decision logic across the component tree.
//!
//! Mutable (appendable) components ignore the profile: their open paths reconstruct
//! in-memory state and cannot be demoted (see `ReadOnlyFieldIndex::preopen`).

use std::collections::HashSet;

use common::universal_io::Populate;

use crate::json_path::JsonPath;
use crate::types::{Condition, Filter, PayloadKeyType, VectorName, VectorNameBuf};

/// Which components of a read-only segment a specific request needs warmed.
///
/// Built from the request via the `for_*` constructors; consumed through the placement
/// methods, which return `None` to keep the persisted-config placement and
/// `Some(`[`Populate::No`]`)` to park the component cold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadProfile {
    /// Vector data the request scores with. The storage, index and quantized vectors of
    /// these keep their configured placement; every other vector's components load cold
    /// (returning stored vectors of a few result points is fine off cold storage).
    warm_vectors: HashSet<VectorNameBuf>,
    /// Payload fields whose indexes the request's filter (or ordering) reads. Their
    /// indexes keep the configured placement; every other field index loads cold.
    warm_payload_fields: HashSet<PayloadKeyType>,
    /// Whether the payload storage keeps its configured placement. Kept warm whenever the
    /// request returns payloads or filters (a condition on an unindexed field falls back
    /// to reading raw payloads); parked cold otherwise.
    warm_payload_storage: bool,
}

impl LoadProfile {
    /// Profile of a scroll: no scoring, so every vector component loads cold; only the
    /// field indexes the filter and `order_by` read stay warm.
    pub fn for_scroll(
        filter: Option<&Filter>,
        order_by_key: Option<&JsonPath>,
        with_payload: bool,
    ) -> Self {
        let mut warm_payload_fields = filter_payload_keys(filter);
        if let Some(key) = order_by_key {
            warm_payload_fields.insert(key.clone());
        }

        Self {
            warm_vectors: HashSet::new(),
            warm_payload_fields,
            warm_payload_storage: with_payload || filter.is_some(),
        }
    }

    /// Profile of a (core) search: the queried vector keeps its configured placement —
    /// storage, index and quantized vectors — and the filter's field indexes stay warm.
    pub fn for_search(
        vector_name: &VectorName,
        filter: Option<&Filter>,
        with_payload: bool,
    ) -> Self {
        Self {
            warm_vectors: HashSet::from([vector_name.to_owned()]),
            warm_payload_fields: filter_payload_keys(filter),
            warm_payload_storage: with_payload || filter.is_some(),
        }
    }

    /// Profile of a retrieve by ids: a handful of point-level random reads, everything
    /// serves fine cold.
    pub fn for_retrieve() -> Self {
        Self {
            warm_vectors: HashSet::new(),
            warm_payload_fields: HashSet::new(),
            warm_payload_storage: false,
        }
    }

    /// Placement override for the payload storage.
    pub fn payload_storage_placement(&self) -> Option<Populate> {
        let Self {
            warm_vectors: _,
            warm_payload_fields: _,
            warm_payload_storage,
        } = self;
        if *warm_payload_storage {
            None
        } else {
            Some(Populate::No)
        }
    }

    /// Placement override for the (dense or sparse) vector storage of `vector_name`.
    pub fn vector_storage_placement(&self, vector_name: &VectorName) -> Option<Populate> {
        self.vector_placement(vector_name)
    }

    /// Placement override for the quantized vectors of `vector_name`.
    ///
    /// Only demotes cache priming: quantized data whose persisted storage kind is a RAM
    /// one is still read in full by the open (the storage kind is part of the persisted
    /// layout, not a placement knob).
    pub fn quantized_vectors_placement(&self, vector_name: &VectorName) -> Option<Populate> {
        self.vector_placement(vector_name)
    }

    /// Placement override for the (dense or sparse) vector index of `vector_name`.
    ///
    /// For HNSW this demotes the graph links to cold residency; an immutable-RAM sparse
    /// index is still materialized by the open (like the quantized RAM storage kinds).
    pub fn vector_index_placement(&self, vector_name: &VectorName) -> Option<Populate> {
        self.vector_placement(vector_name)
    }

    /// Placement override for the payload index of `field`.
    ///
    /// Matches the query path, which resolves a condition against the index of the
    /// condition's exact key: only exact-key matches stay warm.
    pub fn payload_index_placement(&self, field: &JsonPath) -> Option<Populate> {
        let Self {
            warm_vectors: _,
            warm_payload_fields,
            warm_payload_storage: _,
        } = self;
        if warm_payload_fields.contains(field) {
            None
        } else {
            Some(Populate::No)
        }
    }

    /// Shared vector-component decision: the storage, index and quantized vectors of one
    /// vector are warmed (or not) together.
    fn vector_placement(&self, vector_name: &VectorName) -> Option<Populate> {
        let Self {
            warm_vectors,
            warm_payload_fields: _,
            warm_payload_storage: _,
        } = self;
        if warm_vectors.contains(vector_name) {
            None
        } else {
            Some(Populate::No)
        }
    }
}

/// Collect the full payload keys of every field the filter's conditions read, recursing
/// into boolean sub-filters and nested-object conditions (whose inner keys resolve against
/// the nested array prefix, mirroring how the payload index is selected at query time).
fn filter_payload_keys(filter: Option<&Filter>) -> HashSet<PayloadKeyType> {
    let mut keys = HashSet::new();
    if let Some(filter) = filter {
        collect_filter_keys(filter, None, &mut keys);
    }
    keys
}

fn collect_filter_keys(
    filter: &Filter,
    nested_prefix: Option<&JsonPath>,
    keys: &mut HashSet<PayloadKeyType>,
) {
    for condition in filter.iter_conditions() {
        let key = match condition {
            Condition::Field(field_condition) => &field_condition.key,
            Condition::IsEmpty(is_empty) => &is_empty.is_empty.key,
            Condition::IsNull(is_null) => &is_null.is_null.key,
            Condition::Filter(sub_filter) => {
                collect_filter_keys(sub_filter, nested_prefix, keys);
                continue;
            }
            Condition::Nested(nested) => {
                let prefix = JsonPath::extend_or_new(nested_prefix, &nested.array_key());
                collect_filter_keys(nested.filter(), Some(&prefix), keys);
                continue;
            }
            // Id- and vector-level conditions read no payload index.
            Condition::HasId(_) | Condition::HasVector(_) | Condition::CustomIdChecker(_) => {
                continue;
            }
        };
        keys.insert(JsonPath::extend_or_new(nested_prefix, key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldCondition, Match, Nested, NestedCondition, ValueVariants};

    fn path(raw: &str) -> JsonPath {
        raw.parse().unwrap()
    }

    fn match_condition(key: &str, value: &str) -> Condition {
        Condition::Field(FieldCondition::new_match(
            path(key),
            Match::from(ValueVariants::String(value.to_string())),
        ))
    }

    #[test]
    fn scroll_profile_parks_vectors_and_unused_fields_cold() {
        let filter = Filter::new_must(match_condition("city", "London"));
        let profile = LoadProfile::for_scroll(Some(&filter), Some(&path("price")), true);

        assert_eq!(
            profile.vector_storage_placement("dense"),
            Some(Populate::No)
        );
        assert_eq!(profile.vector_index_placement("dense"), Some(Populate::No));
        assert_eq!(
            profile.quantized_vectors_placement("dense"),
            Some(Populate::No)
        );

        assert_eq!(profile.payload_index_placement(&path("city")), None);
        assert_eq!(profile.payload_index_placement(&path("price")), None);
        assert_eq!(
            profile.payload_index_placement(&path("country")),
            Some(Populate::No)
        );

        assert_eq!(profile.payload_storage_placement(), None);
    }

    #[test]
    fn search_profile_keeps_only_queried_vector_warm() {
        let profile = LoadProfile::for_search("dense", None, false);

        assert_eq!(profile.vector_storage_placement("dense"), None);
        assert_eq!(profile.vector_index_placement("dense"), None);
        assert_eq!(profile.quantized_vectors_placement("dense"), None);

        assert_eq!(
            profile.vector_storage_placement("other"),
            Some(Populate::No)
        );
        assert_eq!(profile.vector_index_placement("other"), Some(Populate::No));

        // No payload returned and no filter: payload storage and indexes park cold.
        assert_eq!(profile.payload_storage_placement(), Some(Populate::No));
        assert_eq!(
            profile.payload_index_placement(&path("city")),
            Some(Populate::No)
        );
    }

    #[test]
    fn filter_keys_recurse_into_nested_and_sub_filters() {
        let nested = Condition::Nested(NestedCondition::new(Nested {
            key: path("items"),
            filter: Filter::new_must(match_condition("color", "red")),
        }));
        let sub_filter = Condition::Filter(Filter::new_must(match_condition("country", "UK")));
        let filter = Filter {
            must: Some(vec![nested, sub_filter]),
            must_not: None,
            should: None,
            min_should: None,
        };

        let profile = LoadProfile::for_scroll(Some(&filter), None, false);

        // Nested keys resolve against the array prefix, like index selection at query time.
        assert_eq!(
            profile.payload_index_placement(&path("items[].color")),
            None
        );
        assert_eq!(profile.payload_index_placement(&path("country")), None);
        assert_eq!(
            profile.payload_index_placement(&path("color")),
            Some(Populate::No)
        );

        // A filter is present, so raw payloads may be read for unindexed conditions.
        assert_eq!(profile.payload_storage_placement(), None);
    }

    #[test]
    fn retrieve_profile_loads_everything_cold() {
        let profile = LoadProfile::for_retrieve();

        assert_eq!(
            profile.vector_storage_placement("dense"),
            Some(Populate::No)
        );
        assert_eq!(profile.payload_storage_placement(), Some(Populate::No));
        assert_eq!(
            profile.payload_index_placement(&path("city")),
            Some(Populate::No)
        );
    }
}
