use std::collections::BTreeMap;

use common::types::PointOffsetType;

use super::DeleteLogEntry;
use crate::types::SeqNumberType;

/// The versioned deletes an immutable or disk-resident tracker read but does not apply yet,
/// because their version is above its gate.
#[derive(Debug, Default)]
pub struct HeldDeletes {
    /// Deletes at or below this version apply as soon as they are read. Only ever raised.
    gate: SeqNumberType,
    /// Held slots, each with the version of its delete: the lowest, if a slot was deleted twice.
    held: BTreeMap<PointOffsetType, SeqNumberType>,
}

impl HeldDeletes {
    pub fn new(gate: SeqNumberType) -> Self {
        Self {
            gate,
            held: BTreeMap::new(),
        }
    }

    pub fn gate(&self) -> SeqNumberType {
        self.gate
    }

    pub fn is_held(&self, slot: PointOffsetType) -> bool {
        self.held.contains_key(&slot)
    }

    /// Sort freshly read entries: return the slots to delete now, ascending and deduplicated, and
    /// hold the others.
    pub fn ingest(&mut self, entries: &[DeleteLogEntry]) -> Vec<PointOffsetType> {
        let mut apply = Vec::new();
        for &DeleteLogEntry { slot, version } in entries {
            if version <= self.gate {
                self.held.remove(&slot);
                apply.push(slot);
            } else {
                self.held
                    .entry(slot)
                    .and_modify(|held| *held = (*held).min(version))
                    .or_insert(version);
            }
        }
        apply.sort_unstable();
        apply.dedup();
        apply
    }

    /// Whether [`release`](Self::release) with this gate would release anything.
    pub fn releases_any(&self, gate: SeqNumberType) -> bool {
        self.held.values().any(|&version| version <= gate)
    }

    /// Raise the gate to `gate` and return the held slots it now covers, ascending. They are no
    /// longer held: the caller deletes them.
    pub fn release(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        self.gate = self.gate.max(gate);
        let gate = self.gate;
        let mut released = Vec::new();
        self.held.retain(|&slot, &mut version| {
            let covered = version <= gate;
            if covered {
                released.push(slot);
            }
            !covered
        });
        released
    }
}
