//! Edge read-only follower verification (`edge-verify` cargo feature).
//!
//! Opens each shard directory of the soak collection as an [`edge::ReadOnlyEdgeShard`] —
//! the same reader Qdrant Edge uses to follow a leader-written shard — and reconstructs a
//! model-shaped map from follower scrolls. The run loop compares it against the in-memory
//! model at quiesced checkpoints (op loop idle, snapshot drained, all shards force-flushed),
//! because a follower only ever sees flushed state.
//!
//! Without the feature, [`EdgeVerifier::open`] panics with a rebuild hint, so the
//! `edge_verify` run parameter exists unconditionally and fails loudly instead of silently
//! skipping checks.

#[cfg(feature = "edge-verify")]
use std::collections::BTreeMap;
use std::path::Path;

#[cfg(feature = "edge-verify")]
use common::universal_io::{MmapFile, MmapFs};
#[cfg(feature = "edge-verify")]
use edge::{EdgeShardRead, ReadOnlyEdgeShard, ScrollRequest};
#[cfg(feature = "edge-verify")]
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
#[cfg(feature = "edge-verify")]
use segment::types::{WithPayloadInterface, WithVector};
#[cfg(feature = "edge-verify")]
use shard::retrieve::record_internal::RecordInternal;

use super::Model;
#[cfg(feature = "edge-verify")]
use super::ModelEntry;
#[cfg(feature = "edge-verify")]
use super::VectorValue;
#[cfg(feature = "edge-verify")]
use super::op::canonical_sparse;
#[cfg(feature = "edge-verify")]
use crate::shards::shard_path;

/// One page per follower scroll call; the follower returns a continuation offset.
#[cfg(feature = "edge-verify")]
const SCROLL_PAGE: usize = 1000;

/// Read-only Edge followers over the soak collection's shard directories, one per shard.
/// Opened once and kept across checkpoints, so each [`Self::observe`] exercises
/// `refresh` + `live_reload` deltas (segment churn, in-place appends/deletes) rather than
/// fresh one-shot opens.
pub(super) struct EdgeVerifier {
    #[cfg(feature = "edge-verify")]
    shards: Vec<ReadOnlyEdgeShard<MmapFile>>,
}

impl EdgeVerifier {
    /// Open a follower over every shard directory. Requires the leader to write segment
    /// manifests (`write_segment_manifest` feature flag, set before the collection is
    /// built): the follower discovers segments through the manifest, which is what keeps
    /// discovery safe under optimizer churn — in-construction segments are not listed.
    #[cfg(feature = "edge-verify")]
    pub(super) fn open(collection_dir: &Path, shard_count: u32) -> Self {
        assert!(
            common::flags::feature_flags().write_segment_manifest,
            "edge-verify requires the write_segment_manifest feature flag; \
             set it in init_feature_flags before building the collection",
        );
        let shards = (0..shard_count)
            .map(|shard_id| {
                let path = shard_path(collection_dir, shard_id);
                ReadOnlyEdgeShard::<MmapFile>::open(MmapFs, &path, None, None).unwrap_or_else(|e| {
                    panic!("failed to open edge follower over {}: {e}", path.display())
                })
            })
            .collect();
        Self { shards }
    }

    #[cfg(not(feature = "edge-verify"))]
    pub(super) fn open(_collection_dir: &Path, _shard_count: u32) -> Self {
        panic!(
            "model_testing was built without the `edge-verify` cargo feature; \
             rebuild with `--features edge-verify` to use --edge-verify",
        );
    }

    /// Refresh every follower and scroll the full collection into a model-shaped map.
    /// Only meaningful at a quiesced checkpoint: op loop idle, background snapshot
    /// drained, and every shard force-flushed — the follower sees flushed state only.
    #[cfg(feature = "edge-verify")]
    pub(super) fn observe(&self, ctx: &str) -> Model {
        let mut out = Model::new();
        for follower in &self.shards {
            follower.refresh().unwrap_or_else(|e| {
                panic!(
                    "{ctx}: follower refresh of {} failed: {e}",
                    follower.path().display(),
                )
            });
            let mut offset = None;
            loop {
                let (records, next) = follower
                    .scroll(ScrollRequest {
                        offset,
                        limit: Some(SCROLL_PAGE),
                        filter: None,
                        with_payload: Some(WithPayloadInterface::Bool(true)),
                        with_vector: WithVector::Bool(true),
                        order_by: None,
                    })
                    .unwrap_or_else(|e| {
                        panic!(
                            "{ctx}: follower scroll of {} failed: {e}",
                            follower.path().display(),
                        )
                    });
                for record in records {
                    let id = record.id;
                    if out.insert(id, record_to_model_entry(record, ctx)).is_some() {
                        panic!("{ctx}: point {id:?} returned by more than one shard follower");
                    }
                }
                match next {
                    Some(next_offset) => offset = Some(next_offset),
                    None => break,
                }
            }
        }
        out
    }

    // `&self` mirrors the real method so call sites compile identically in both configs.
    #[allow(clippy::unused_self)]
    #[cfg(not(feature = "edge-verify"))]
    pub(super) fn observe(&self, _ctx: &str) -> Model {
        unreachable!("EdgeVerifier cannot be constructed without the edge-verify feature")
    }
}

/// Convert one follower scroll record into the model's entry shape: named vectors with
/// sparse canonicalized (the engine returns them sorted, but canonicalizing here keeps the
/// conversion total) and multi-dense flattened to rows; a missing payload is the empty one.
#[cfg(feature = "edge-verify")]
fn record_to_model_entry(record: RecordInternal, ctx: &str) -> ModelEntry {
    let RecordInternal {
        id,
        payload,
        vector,
        shard_key: _,
        order_value: _,
    } = record;
    let named = match vector {
        Some(VectorStructInternal::Named(named)) => named,
        Some(other @ (VectorStructInternal::Single(_) | VectorStructInternal::MultiDense(_))) => {
            panic!("{ctx}: expected named vectors for {id:?}, got {other:?}")
        }
        None => panic!("{ctx}: vector missing in follower scroll result for {id:?}"),
    };
    let mut vectors = BTreeMap::new();
    for (name, value) in named {
        let v = match value {
            VectorInternal::Dense(v) => VectorValue::Dense(v),
            VectorInternal::Sparse(sv) => VectorValue::Sparse(canonical_sparse(&sv)),
            VectorInternal::MultiDense(m) => {
                VectorValue::MultiDense(m.multi_vectors().map(<[f32]>::to_vec).collect())
            }
        };
        vectors.insert(name, v);
    }
    ModelEntry {
        vectors,
        payload: payload.unwrap_or_default(),
    }
}

#[cfg(all(test, feature = "edge-verify"))]
mod tests {
    use std::collections::HashMap;

    use segment::data_types::vectors::{
        MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
    };
    use segment::types::{Payload, PointIdType};
    use shard::retrieve::record_internal::RecordInternal;
    use sparse::common::sparse_vector::SparseVector;

    use super::record_to_model_entry;
    use crate::model_testing::VectorValue;

    /// Sparse vectors canonicalize (sorted indices, zero entries dropped), multi-dense
    /// flattens to rows, and a missing payload becomes the empty payload — matching how
    /// the model stores entries.
    #[test]
    fn record_conversion_matches_model_shape() {
        let record = RecordInternal {
            id: PointIdType::NumId(7),
            payload: None,
            vector: Some(VectorStructInternal::Named(HashMap::from([
                ("a".to_string(), VectorInternal::Dense(vec![1.0, 2.0])),
                (
                    "s".to_string(),
                    VectorInternal::Sparse(SparseVector {
                        indices: vec![5, 2, 9],
                        values: vec![0.5, 0.0, 1.5],
                    }),
                ),
                (
                    "m".to_string(),
                    VectorInternal::MultiDense(MultiDenseVectorInternal::new(
                        vec![1.0, 2.0, 3.0, 4.0],
                        2,
                    )),
                ),
            ]))),
            shard_key: None,
            order_value: None,
        };

        let entry = record_to_model_entry(record, "test");

        assert_eq!(entry.vectors["a"], VectorValue::Dense(vec![1.0, 2.0]));
        // (2, 0.0) dropped, remaining pairs sorted by index.
        assert_eq!(
            entry.vectors["s"],
            VectorValue::Sparse(SparseVector {
                indices: vec![5, 9],
                values: vec![0.5, 1.5],
            }),
        );
        assert_eq!(
            entry.vectors["m"],
            VectorValue::MultiDense(vec![vec![1.0, 2.0], vec![3.0, 4.0]]),
        );
        assert_eq!(entry.payload, Payload::default());
    }
}
