//! Pending vector-schema changes recorded in a [`super::ProxySegment`].
//!
//! See [`IntendedVector`] for the rationale behind the intent representation
//! and [`ProxyVectorNameChanges`] for the per-proxy buffer.

use ahash::AHashMap;
use itertools::Itertools as _;
use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::types::{
    SegmentConfig, SeqNumberType, SparseVectorDataConfig, VectorDataConfig, VectorName,
    VectorNameBuf,
};

/// Desired end-state of a single named vector inside a [`super::ProxySegment`].
///
/// `ProxyVectorNameChanges` records, for each name touched in the proxy, the
/// final state we want the wrapped/optimised segment to converge to once the
/// proxy is drained. The previous representation was a Create-or-Delete pair
/// stored in a HashMap, which silently collapsed `Delete v then Create v with
/// a different schema` into a plain `Create v` and let the optimiser keep the
/// stale storage from the wrapped segment. The intent representation keeps
/// enough information that the apply path can clear that stale storage before
/// installing the new schema.
#[derive(Debug, Clone)]
pub enum IntendedVector {
    /// The vector should exist with this configuration once the proxy drains.
    Present {
        config: VectorNameConfig,
        version: SeqNumberType,
        /// `true` iff the wrapped segment carries this name with a *different*
        /// schema (or as the wrong kind), or has been touched by an earlier
        /// `Absent` intent in this proxy. When this flag is set, any code that
        /// re-applies the change must clear the existing vector data before
        /// installing the new config — otherwise the idempotent
        /// `create_vector_name_impl` will silently keep the old storage.
        supersedes_wrapped: bool,
    },
    /// The vector has been deleted in the proxy.
    Absent { version: SeqNumberType },
}

impl IntendedVector {
    pub fn version(&self) -> SeqNumberType {
        match self {
            IntendedVector::Present { version, .. } => *version,
            IntendedVector::Absent { version } => *version,
        }
    }

    /// Whether the wrapped segment's data for this vector is no longer
    /// authoritative and must not be migrated as-is by the optimiser.
    fn taints_wrapped(&self) -> bool {
        matches!(
            self,
            IntendedVector::Absent { .. }
                | IntendedVector::Present {
                    supersedes_wrapped: true,
                    ..
                }
        )
    }
}

#[derive(Debug, Default)]
pub struct ProxyVectorNameChanges {
    intent: AHashMap<VectorNameBuf, IntendedVector>,
}

impl ProxyVectorNameChanges {
    /// Record a `Create` intent for `vector_name`.
    ///
    /// `wrapped_config` is the segment config the proxy is wrapping; it is used
    /// to decide whether the wrapped segment already carries this name with a
    /// matching schema (in which case the new entry can leave
    /// `supersedes_wrapped = false` and the optimiser's idempotent create is
    /// fine) or with a stale schema / wrong kind (in which case the flag is
    /// set so the apply path knows to delete first).
    pub fn record_create(
        &mut self,
        vector_name: VectorNameBuf,
        config: VectorNameConfig,
        version: SeqNumberType,
        wrapped_config: &SegmentConfig,
    ) {
        // Carry forward an earlier "tainted" flag: a previous `Absent` (or a
        // previous `Present { supersedes_wrapped: true }`) means the wrapped
        // data has already been logically discarded by this proxy, so even a
        // same-schema re-create cannot resurrect it.
        let previous_taints = self
            .intent
            .get(&vector_name)
            .is_some_and(IntendedVector::taints_wrapped);
        let supersedes_wrapped =
            previous_taints || wrapped_carries_stale_schema(wrapped_config, &vector_name, &config);

        self.intent.insert(
            vector_name,
            IntendedVector::Present {
                config,
                version,
                supersedes_wrapped,
            },
        );
    }

    /// Record a `Delete` intent for `vector_name`.
    pub fn record_delete(&mut self, vector_name: VectorNameBuf, version: SeqNumberType) {
        self.intent
            .insert(vector_name, IntendedVector::Absent { version });
    }

    pub fn is_empty(&self) -> bool {
        self.intent.is_empty()
    }

    pub fn clear(&mut self) {
        self.intent.clear();
    }

    /// Iterate over proxied vector name intents in order of version.
    ///
    /// Intents must be applied in version order: an intent with a stale version
    /// will be silently rejected by the target segment.
    pub fn iter_ordered(&self) -> impl Iterator<Item = (&VectorNameBuf, &IntendedVector)> {
        self.intent
            .iter()
            .sorted_by_key(|(_, intent)| intent.version())
    }

    /// Merge intents from another proxy into this one. Per name, the higher
    /// version wins; if either side carried a `taints_wrapped` flag, it is
    /// propagated onto the winner so the apply path errs on the side of
    /// clearing stale data.
    pub fn merge(&mut self, other: &Self) {
        for (name, other_intent) in &other.intent {
            let other_taints = other_intent.taints_wrapped();
            match self.intent.get_mut(name) {
                None => {
                    self.intent.insert(name.clone(), other_intent.clone());
                }
                Some(self_intent) => {
                    if other_intent.version() > self_intent.version() {
                        let self_taints = self_intent.taints_wrapped();
                        let mut winner = other_intent.clone();
                        if self_taints
                            && let IntendedVector::Present {
                                supersedes_wrapped, ..
                            } = &mut winner
                        {
                            *supersedes_wrapped = true;
                        }
                        *self_intent = winner;
                    } else if other_taints
                        && let IntendedVector::Present {
                            supersedes_wrapped, ..
                        } = self_intent
                    {
                        *supersedes_wrapped = true;
                    }
                }
            }
        }
    }
}

/// Returns `true` iff the wrapped segment carries `vector_name` with a schema
/// that does **not** match the requested `new_config` — including the
/// kind-mismatch case (wrapped has it as dense, new is sparse, or vice versa).
///
/// The check intentionally only looks at user-controlled, immutable schema
/// fields (`size` / `distance` / `multivector_config` / `datatype` for dense,
/// `modifier` / `index.datatype` for sparse) and ignores tunable fields like
/// HNSW or quantization config that can be updated independently after
/// creation. A wrapped segment that doesn't have the name at all has nothing
/// stale to carry, so the function returns `false` in that case.
fn wrapped_carries_stale_schema(
    wrapped_config: &SegmentConfig,
    vector_name: &VectorName,
    new_config: &VectorNameConfig,
) -> bool {
    match new_config {
        VectorNameConfig::Dense(wrapper) => {
            // Wrong kind — wrapped stores it as a sparse vector.
            if wrapped_config.sparse_vector_data.contains_key(vector_name) {
                return true;
            }
            let Some(existing) = wrapped_config.vector_data.get(vector_name) else {
                return false;
            };
            !wrapped_dense_schema_matches(existing, &wrapper.dense)
        }
        VectorNameConfig::Sparse(wrapper) => {
            // Wrong kind — wrapped stores it as a dense vector.
            if wrapped_config.vector_data.contains_key(vector_name) {
                return true;
            }
            let Some(existing) = wrapped_config.sparse_vector_data.get(vector_name) else {
                return false;
            };
            !wrapped_sparse_schema_matches(existing, &wrapper.sparse)
        }
    }
}

fn wrapped_dense_schema_matches(
    existing: &VectorDataConfig,
    new_config: &DenseVectorConfig,
) -> bool {
    let DenseVectorConfig {
        size,
        distance,
        multivector_config,
        datatype,
    } = new_config;
    existing.size == *size
        && existing.distance == *distance
        && existing.multivector_config == *multivector_config
        && existing.datatype == *datatype
}

fn wrapped_sparse_schema_matches(
    existing: &SparseVectorDataConfig,
    new_config: &SparseVectorConfig,
) -> bool {
    let SparseVectorConfig { modifier, datatype } = new_config;
    existing.modifier == *modifier && existing.index.datatype == *datatype
}
