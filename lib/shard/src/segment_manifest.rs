//! The segment manifest (`segments/manifest.json`): a small file listing the shard's segments and
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
/// Only [`Active`](SegmentManifestState::Active) is written today. The other variants are defined so
/// the on-disk format can grow to describe in-progress segment transitions without breaking
/// compatibility for readers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentManifestState {
    /// Live segment, part of the shard's data and safe to read.
    Active,
    /// Reserved for future use: a segment being built that is not yet ready to read.
    UnderConstruction,
    /// Reserved for future use: a superseded segment that is still readable but pending removal.
    Retiring,
}

/// Contents of `segments/manifest.json`: a flat map of segment UUID to its state, e.g.
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

    fn add_extra(&mut self, uuid: Uuid, state: SegmentManifestState) {
        self.segments.insert(uuid, state);
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

        let mut current = Self::from_segment_holder(holder);
        if let Some(uuid) = extra_segment {
            current.add_extra(uuid, SegmentManifestState::Active);
        }

        if *manifest.read() == current {
            return Ok(());
        }

        manifest.write(|m| *m = current).map_err(|err| {
            OperationError::service_error(format!("failed to persist segment manifest: {err}"))
        })?;
        Ok(())
    }

    pub fn get(&self, uuid: &Uuid) -> Option<SegmentManifestState> {
        self.segments.get(uuid).copied()
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
}
