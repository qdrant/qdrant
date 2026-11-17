//! Versioned deletes an appendable tracker reads from its mappings log but holds, because their
//! version is above its gate. See [`delete_versions`](crate::id_tracker::delete_versions).

use common::types::PointOffsetType;
use smallvec::SmallVec;

use super::ReadOnlyAppendableIdTracker;
use crate::id_tracker::delete_versions::DeleteGating;
use crate::types::{PointIdType, SeqNumberType};

/// The held deletes of a tracker that gates them.
#[derive(Debug)]
pub(super) struct HeldMappingDeletes {
    /// Deletes at or below this version apply as soon as they are read. Only ever raised.
    gate: SeqNumberType,
    /// In log order.
    held: Vec<HeldMappingDelete>,
}

/// A delete of `external_id` in the log, applying to the slots the point held right before it:
/// linked, staged or still waiting for its version. A slot the point gets after it stays.
#[derive(Debug)]
struct HeldMappingDelete {
    external_id: PointIdType,
    version: SeqNumberType,
    /// Usually just one.
    slots: SmallVec<[PointOffsetType; 1]>,
}

impl HeldMappingDeletes {
    /// `None` for [`DeleteGating::Immediate`], which holds nothing.
    pub(super) fn for_gating(gating: DeleteGating) -> Option<Self> {
        match gating {
            DeleteGating::Immediate => None,
            DeleteGating::Held { gate } => Some(Self {
                gate,
                held: Vec::new(),
            }),
        }
    }

    /// Whether a delete of this version applies as soon as it is read.
    pub(super) fn covers(&self, version: SeqNumberType) -> bool {
        version <= self.gate
    }
}

impl<S: common::universal_io::UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Hold a versioned delete of `external_id` read from the log: its slots stay as they are.
    pub(super) fn hold_delete(&mut self, external_id: PointIdType, version: SeqNumberType) {
        let mut slots: SmallVec<[PointOffsetType; 1]> = self.mappings.heads(&external_id).collect();
        slots.extend(self.staged_inserts.get(&external_id).copied());
        if let Some(pending) = self.unversioned_inserts.get(&external_id) {
            slots.extend(pending.iter().copied());
        }
        slots.sort_unstable();
        slots.dedup();

        let held = self
            .held_deletes
            .as_mut()
            .expect("only a tracker that gates deletes holds them");
        held.held.push(HeldMappingDelete {
            external_id,
            version,
            slots,
        });
    }

    /// Whether [`release_held_deletes`](Self::release_held_deletes) with this gate would release
    /// anything.
    pub fn releases_held_deletes(&self, gate: SeqNumberType) -> bool {
        self.held_deletes
            .as_ref()
            .is_some_and(|held| held.held.iter().any(|delete| delete.version <= gate))
    }

    /// Raise the gate to `gate`, and apply the held deletes it now covers, in log order. Returns
    /// the visible offsets they removed, sorted ascending, for the components to delete too.
    pub fn release_held_deletes(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        let Some(held) = self.held_deletes.as_mut() else {
            return Vec::new();
        };
        held.gate = held.gate.max(gate);
        let gate = held.gate;

        let mut released = Vec::new();
        let mut kept = Vec::with_capacity(held.held.len());
        for delete in std::mem::take(&mut held.held) {
            if delete.version > gate {
                kept.push(delete);
                continue;
            }
            let HeldMappingDelete {
                external_id,
                version: _,
                slots,
            } = delete;
            for slot in slots {
                if self.mappings.drop_head(external_id, slot) {
                    released.push(slot);
                }
                // Staged offsets are reported as inserted until published, so they are deleted
                // too, an insert still waiting for its version is just forgotten
                if self.staged_inserts.get(&external_id) == Some(&slot) {
                    self.staged_inserts.remove(&external_id);
                    released.push(slot);
                }
                if let Some(pending) = self.unversioned_inserts.get_mut(&external_id) {
                    pending.retain(|pending| *pending != slot);
                    if pending.is_empty() {
                        self.unversioned_inserts.remove(&external_id);
                    }
                }
            }
        }
        self.held_deletes.as_mut().expect("checked above").held = kept;

        released.sort_unstable();
        released.dedup();
        released
    }
}
