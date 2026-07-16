//! The segment manifest (`segments_manifest.json`, sitting next to the `segments/` directory): a
//! small file listing the shard's segments and
//! their state, so out-of-process readers (e.g. a read-only follower, possibly over object storage)
//! can discover segments without scanning the filesystem.
//!
//! Defines the on-disk structure and a helper to build it from a [`SegmentHolder`]. The *logic* of
//! when to write it lives in the owner of the segments (e.g. `LocalShard`).

use std::collections::HashMap;
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use segment::common::operation_error::{OperationError, OperationResult};
// Re-exported from `segment` (where `build_segment` mints it) since the manifest is what consumes it.
pub use segment::segment_constructor::NewSegmentToken;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::segment_holder::SegmentHolder;

/// State of a segment in the manifest.
///
/// The shard itself only writes [`Active`](SegmentManifestState::Active); the in-progress states
/// come from out-of-process rewriters (the serverless indexer). A data-carrying state serializes
/// as a tagged object, which readers predating it cannot parse — write it only once every reader
/// understands it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentManifestState {
    /// Live segment, part of the shard's data and safe to read.
    Active,
    /// Reserved for future use: a segment being built that is not yet ready to read.
    UnderConstruction,
    /// Being rebuilt by an out-of-process optimizer; still live and readable. The claim is a
    /// lease: past `lease_until` another optimizer may take the segment over.
    Optimizing {
        /// Identity of the optimizer run holding the claim (e.g. pod name).
        holder: String,
        /// Unix seconds after which the claim is stale.
        lease_until: u64,
    },
    /// A superseded segment that is still readable but pending removal.
    Retiring,
}

/// Contents of `segments_manifest.json`: a flat map of segment UUID to its state, e.g.
/// `{ "1b4e28ba-...": "active", "6ba7b810-...": "active" }`.
///
/// # Consistency assumptions (important for readers)
///
/// The manifest is *not* an exact, instantaneous snapshot of the on-disk segment set. To guarantee
/// that no data is ever lost across the non-atomic create/delete transitions, it deliberately errs
/// on the side of listing more segments than strictly necessary:
///
/// - **May list a segment that is not yet fully created.** A new segment is registered here as soon
///   as it exists on disk — potentially *before* its version file is written (i.e. before it is
///   finalized). This guarantees a real, writable segment is never missing from the manifest. A
///   reader must therefore tolerate a listed segment that is incomplete / not yet loadable and skip
///   it (a segment without a saved version is not ready to read).
/// - **May list a segment that has already been (or is about to be) deleted.** Removal from the
///   manifest is intentionally not strict: a superseded segment may linger in the manifest after its
///   data has been dropped, or be dropped from disk slightly after it leaves the manifest. A reader
///   must tolerate a listed segment that no longer exists on disk and skip it.
///
/// In other words: the manifest is a *superset-biased* view. Over-listing (duplicate/extra segments)
/// is always safe and is resolved by point versioning during reads; under-listing a live segment is
/// the only thing that would lose data, and the writer ordering prevents it. Readers must handle
/// both of the above cases gracefully rather than assuming every listed segment is present and ready.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentsManifest {
    segments: HashMap<Uuid, SegmentManifestState>,
}

impl SegmentsManifest {
    /// Build a manifest from the current live segments in the holder, marking every segment
    /// [`Active`](SegmentManifestState::Active).
    ///
    /// Includes segments temporarily wrapped as proxies during optimization — they are still live.
    pub fn from_segment_holder(holder: &SegmentHolder) -> Self {
        let segments = holder
            .iter()
            .map(|(_, locked_segment)| {
                let uuid = locked_segment.get_read().read().segment_uuid();
                (uuid, SegmentManifestState::Active)
            })
            .collect();
        Self { segments }
    }

    /// Set `uuid`'s state, returning the previous one.
    pub fn set(&mut self, uuid: Uuid, state: SegmentManifestState) -> Option<SegmentManifestState> {
        self.segments.insert(uuid, state)
    }

    /// Remove `uuid`'s entry, returning its state.
    pub fn remove(&mut self, uuid: &Uuid) -> Option<SegmentManifestState> {
        self.segments.remove(uuid)
    }

    /// Keep `previous`'s in-progress marks (`Optimizing`/`Retiring`) for segments this manifest
    /// also lists: a holder rebuild marks everything `Active`, which would erase another
    /// process's claim. Staleness is governed by the lease, not by rebuilds.
    #[must_use]
    pub fn preserving(mut self, previous: &SegmentsManifest) -> Self {
        for (uuid, state) in previous.iter() {
            let in_progress = match state {
                SegmentManifestState::Optimizing {
                    holder: _,
                    lease_until: _,
                }
                | SegmentManifestState::Retiring => true,
                SegmentManifestState::Active | SegmentManifestState::UnderConstruction => false,
            };
            if in_progress && self.segments.contains_key(uuid) {
                self.segments.insert(*uuid, state.clone());
            }
        }
        self
    }

    /// Rebuild the manifest from `holder` and persist it if it changed.
    ///
    /// No-op when `manifest` is `None` (the `write_segment_manifest` flag is off). Errors
    /// propagate so callers that gate destructive work (e.g. deleting superseded segments) on a
    /// fresh manifest can abort instead of risking a stale manifest.
    ///
    /// Idempotent and cheap: it only writes when the live segment set differs from what is
    /// persisted, so it is safe to call on every relevant segment-set transition. Usually invoked
    /// via [`SegmentHolder::sync_segment_manifest`], which owns the manifest.
    pub fn sync(
        manifest: Option<&Arc<SaveOnDisk<SegmentsManifest>>>,
        holder: &SegmentHolder,
        extra_segment: Option<Uuid>,
    ) -> OperationResult<()> {
        let Some(manifest) = manifest else {
            return Ok(());
        };

        let mut rebuilt = Self::from_segment_holder(holder);
        if let Some(uuid) = extra_segment {
            rebuilt.set(uuid, SegmentManifestState::Active);
        }

        // Merge under the write lock, so a concurrent rebuild cannot slip between a
        // read and a write and erase preserved marks.
        manifest
            .write_optional(|previous| {
                let current = rebuilt.preserving(previous);
                (*previous != current).then_some(current)
            })
            .map_err(|err| {
                OperationError::service_error(format!("failed to persist segment manifest: {err}"))
            })?;
        Ok(())
    }

    pub fn get(&self, uuid: &Uuid) -> Option<SegmentManifestState> {
        self.segments.get(uuid).cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Uuid, &SegmentManifestState)> {
        self.segments.iter()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

impl FromIterator<(Uuid, SegmentManifestState)> for SegmentsManifest {
    fn from_iter<I: IntoIterator<Item = (Uuid, SegmentManifestState)>>(iter: I) -> Self {
        Self {
            segments: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_active_as_snake_case_uuid_map() {
        let uuid = Uuid::parse_str("1b4e28ba-2fa1-11d2-883f-0016d3cca427").unwrap();
        let manifest = SegmentsManifest {
            segments: HashMap::from([(uuid, SegmentManifestState::Active)]),
        };

        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(json, r#"{"1b4e28ba-2fa1-11d2-883f-0016d3cca427":"active"}"#);

        let parsed: SegmentsManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.get(&uuid), Some(SegmentManifestState::Active));
    }

    #[test]
    fn deserializes_future_states_for_forward_compat() {
        let active = Uuid::parse_str("1b4e28ba-2fa1-11d2-883f-0016d3cca427").unwrap();
        let building = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let retiring = Uuid::parse_str("6ba7b811-9dad-11d1-80b4-00c04fd430c8").unwrap();

        let json = format!(
            r#"{{"{active}":"active","{building}":"under_construction","{retiring}":"retiring"}}"#,
        );
        let parsed: SegmentsManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.get(&active), Some(SegmentManifestState::Active));
        assert_eq!(
            parsed.get(&building),
            Some(SegmentManifestState::UnderConstruction),
        );
        assert_eq!(parsed.get(&retiring), Some(SegmentManifestState::Retiring));
    }

    #[test]
    fn optimizing_roundtrips_as_tagged_object() {
        let uuid = Uuid::parse_str("1b4e28ba-2fa1-11d2-883f-0016d3cca427").unwrap();
        let manifest: SegmentsManifest = [(
            uuid,
            SegmentManifestState::Optimizing {
                holder: "indexer-1".to_string(),
                lease_until: 1_752_000_000,
            },
        )]
        .into_iter()
        .collect();

        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(
            json,
            r#"{"1b4e28ba-2fa1-11d2-883f-0016d3cca427":{"optimizing":{"holder":"indexer-1","lease_until":1752000000}}}"#,
        );

        let parsed: SegmentsManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, manifest);
    }

    /// Byte-compat guard: today's writers produce bare strings for the unit states; adding
    /// the data-carrying variant must not change how those serialize.
    #[test]
    fn unit_states_keep_their_bare_string_form() {
        let uuid = Uuid::parse_str("6ba7b811-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let manifest: SegmentsManifest = [(uuid, SegmentManifestState::Retiring)]
            .into_iter()
            .collect();
        assert_eq!(
            serde_json::to_string(&manifest).unwrap(),
            r#"{"6ba7b811-9dad-11d1-80b4-00c04fd430c8":"retiring"}"#,
        );
    }

    #[test]
    fn preserving_keeps_in_progress_marks_for_listed_segments_only() {
        let kept = Uuid::parse_str("1b4e28ba-2fa1-11d2-883f-0016d3cca427").unwrap();
        let gone = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let fresh = Uuid::parse_str("6ba7b811-9dad-11d1-80b4-00c04fd430c8").unwrap();

        let optimizing = SegmentManifestState::Optimizing {
            holder: "indexer-1".to_string(),
            lease_until: 42,
        };
        let previous: SegmentsManifest = [
            (kept, optimizing.clone()),
            (gone, SegmentManifestState::Retiring),
        ]
        .into_iter()
        .collect();

        // A holder rebuild lists every live segment as Active; `gone` left the holder.
        let rebuilt: SegmentsManifest = [
            (kept, SegmentManifestState::Active),
            (fresh, SegmentManifestState::Active),
        ]
        .into_iter()
        .collect();

        let merged = rebuilt.preserving(&previous);
        assert_eq!(merged.get(&kept), Some(optimizing));
        assert_eq!(merged.get(&fresh), Some(SegmentManifestState::Active));
        assert_eq!(merged.get(&gone), None, "marks drop with the segment");
    }
}
