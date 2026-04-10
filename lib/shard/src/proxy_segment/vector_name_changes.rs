//! Pending vector-schema changes recorded in a [`super::ProxySegment`].
//!
//! See [`IntendedVector`] for the rationale behind the intent representation
//! and [`ProxyVectorNameChanges`] for the per-proxy buffer.

use std::borrow::Cow;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use itertools::Itertools as _;
use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::index::field_index::CardinalityEstimation;
use segment::types::{
    Condition, CustomIdCheckerCondition, ExtendedPointId, Filter, SegmentConfig, SeqNumberType,
    SparseVectorDataConfig, VectorDataConfig, VectorName, VectorNameBuf, WithVector,
};

/// A [`CustomIdCheckerCondition`] that never matches any point. Used to
/// replace `HasVector` conditions that reference a vector the proxy has
/// deleted or superseded — the wrapped segment's storage for that vector is
/// stale, so the condition must evaluate to `false` for every point.
#[derive(Debug)]
struct AlwaysFalseChecker;

impl CustomIdCheckerCondition for AlwaysFalseChecker {
    fn estimate_cardinality(&self, _points: usize) -> CardinalityEstimation {
        CardinalityEstimation::exact(0)
    }

    fn check(&self, _point_id: ExtendedPointId) -> bool {
        false
    }
}

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

    /// Whether the wrapped segment's data for `vector_name` is no longer
    /// authoritative — i.e. the proxy queues a `Delete` (`Absent`) or a
    /// `Create` that supersedes the wrapped's existing schema. Read paths
    /// in `ProxySegment` use this to short-circuit lookups against the
    /// wrapped segment and return empty results instead of stale data.
    ///
    /// Returns `false` for names not touched by the proxy at all (the
    /// wrapped is the source of truth) and for `Present { supersedes_wrapped:
    /// false }` (a same-schema re-create or a brand-new name where wrapped
    /// has nothing to be stale about).
    pub fn is_wrapped_data_stale(&self, vector_name: &VectorName) -> bool {
        self.intent
            .get(vector_name)
            .is_some_and(IntendedVector::taints_wrapped)
    }

    /// Drop any vector names from `with_vector` whose data the wrapped segment
    /// can no longer be trusted to serve — either because the proxy intends
    /// to delete them outright (`Absent`) or because it intends to replace
    /// them with a different schema (`Present { supersedes_wrapped: true }`).
    /// Read paths in `ProxySegment` use this to rewrite the parameter before
    /// delegating to the wrapped segment, so a request like
    /// `WithVector::Selector(["v_dropped"])` doesn't bring back stale data.
    ///
    /// `wrapped_config` is the config of the segment this proxy is wrapping;
    /// it is the source of truth for "what does `Bool(true)` (= all vectors)
    /// expand to?". It's passed in rather than stored on the buffer because
    /// (a) `ProxySegment` already owns the canonical copy at
    /// `self.wrapped_config`, (b) `ProxyVectorNameChanges` is a pure delta
    /// buffer and adding base state to it would muddy its role and break
    /// `merge` across proxies that wrap different segments, and (c) every
    /// call site is inside `impl ProxySegment` and already has the config in
    /// scope.
    ///
    /// Returns [`Cow::Borrowed`] when nothing needs to change (no tainted
    /// names, or the request is `Bool(false)`, or none of the explicitly
    /// requested names are tainted) and [`Cow::Owned`] only when at least
    /// one name was actually dropped or `Bool(true)` had to be expanded to a
    /// `Selector`.
    pub fn redact_with_vector<'a>(
        &self,
        with_vector: &'a WithVector,
        wrapped_config: &SegmentConfig,
    ) -> Cow<'a, WithVector> {
        let tainted: AHashSet<&str> = self
            .intent
            .iter()
            .filter(|(_, intent)| intent.taints_wrapped())
            .map(|(name, _)| name.as_str())
            .collect();

        if tainted.is_empty() {
            return Cow::Borrowed(with_vector);
        }

        match with_vector {
            // Nothing requested — nothing to redact.
            WithVector::Bool(false) => Cow::Borrowed(with_vector),

            // "All vectors" expands to whatever the wrapped knows minus the
            // tainted set. Pending Creates of brand-new names are deliberately
            // excluded: the wrapped has no data for them anyway, and asking
            // it for them would error out.
            WithVector::Bool(true) => {
                let kept: Vec<VectorNameBuf> = wrapped_config
                    .vector_data
                    .keys()
                    .chain(wrapped_config.sparse_vector_data.keys())
                    .filter(|name| !tainted.contains(name.as_str()))
                    .cloned()
                    .collect();
                Cow::Owned(WithVector::Selector(kept))
            }

            // Filter the explicit list. Skip the allocation if no requested
            // name is actually tainted (the common case for an unrelated
            // schema change happening in the background).
            WithVector::Selector(requested) => {
                let needs_redact = requested.iter().any(|name| tainted.contains(name.as_str()));
                if !needs_redact {
                    return Cow::Borrowed(with_vector);
                }
                let kept: Vec<VectorNameBuf> = requested
                    .iter()
                    .filter(|name| !tainted.contains(name.as_str()))
                    .cloned()
                    .collect();
                Cow::Owned(WithVector::Selector(kept))
            }
        }
    }

    /// Rewrite a [`Filter`] so that any `HasVector` condition that references
    /// a deleted or superseded vector name is replaced with a
    /// [`Condition::CustomIdChecker`] that always returns `false`. This makes
    /// queries like `must: [{ has_vector: "v_dropped" }]` correctly match
    /// zero points in the proxy, instead of leaking through to the wrapped
    /// segment's stale storage.
    ///
    /// Returns [`Cow::Borrowed`] when no `HasVector` in the filter tree
    /// references a tainted name (the common case) and [`Cow::Owned`] only
    /// when at least one condition was rewritten. The scan is a cheap
    /// read-only pass; cloning only happens if a rewrite is actually needed.
    pub fn redact_filter<'a>(&self, filter: &'a Filter) -> Cow<'a, Filter> {
        if !self.filter_has_stale_has_vector(filter) {
            return Cow::Borrowed(filter);
        }
        let mut owned = filter.clone();
        self.redact_filter_inplace(&mut owned);
        Cow::Owned(owned)
    }

    /// Recursive read-only scan: does the filter tree contain at least one
    /// `HasVector` whose name is tainted?
    fn filter_has_stale_has_vector(&self, filter: &Filter) -> bool {
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = filter;

        let conditions = should
            .iter()
            .flatten()
            .chain(must.iter().flatten())
            .chain(must_not.iter().flatten())
            .chain(min_should.iter().flat_map(|ms| ms.conditions.iter()));
        for cond in conditions {
            match cond {
                Condition::HasVector(hv) => {
                    if self.is_wrapped_data_stale(&hv.has_vector) {
                        return true;
                    }
                }
                Condition::Nested(nested) => {
                    if self.filter_has_stale_has_vector(&nested.nested.filter) {
                        return true;
                    }
                }
                Condition::Filter(inner) => {
                    if self.filter_has_stale_has_vector(inner) {
                        return true;
                    }
                }
                Condition::Field(_) => {}
                Condition::IsEmpty(_) => {}
                Condition::IsNull(_) => {}
                Condition::HasId(_) => {}
                Condition::CustomIdChecker(_) => {}
            }
        }
        false
    }

    /// Walk a cloned filter tree in-place, replacing tainted `HasVector`
    /// conditions with an always-false checker.
    fn redact_filter_inplace(&self, filter: &mut Filter) {
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = filter;

        if let Some(conds) = should {
            self.redact_conditions_inplace(conds);
        }
        if let Some(conds) = must {
            self.redact_conditions_inplace(conds);
        }
        if let Some(conds) = must_not {
            self.redact_conditions_inplace(conds);
        }
        if let Some(ms) = min_should {
            self.redact_conditions_inplace(&mut ms.conditions);
        }
    }

    fn redact_conditions_inplace(&self, conditions: &mut [Condition]) {
        for cond in conditions.iter_mut() {
            match cond {
                Condition::HasVector(hv) => {
                    if self.is_wrapped_data_stale(&hv.has_vector) {
                        *cond = Condition::new_custom(Arc::new(AlwaysFalseChecker));
                    }
                }
                Condition::Nested(nested) => {
                    self.redact_filter_inplace(&mut nested.nested.filter);
                }
                Condition::Filter(inner) => {
                    self.redact_filter_inplace(inner);
                }
                Condition::Field(_) => {}
                Condition::IsEmpty(_) => {}
                Condition::IsNull(_) => {}
                Condition::HasId(_) => {}
                Condition::CustomIdChecker(_) => {}
            }
        }
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
