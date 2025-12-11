use ahash::AHashSet;
use smallvec::SmallVec;

use crate::tracker::ValuePointer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct VersionedPointer {
    /// Lightweight version for this pointer update
    opnum: u64,
    /// The pointer pending for freeing
    pointer: ValuePointer,
}

impl VersionedPointer {
    #[cfg(test)]
    fn new_raw(opnum: u64, page_id: u32, block_offset: u32, length: u32) -> Self {
        VersionedPointer {
            opnum,
            pointer: ValuePointer::new(page_id, block_offset, length),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(super) struct PointerUpdates {
    /// Whether the latest pointer is set (`true`) or unset (`false`).
    /// If this is `true`, then history must have at least one element.
    latest_is_set: bool,
    /// List of pointers where the value has been written.
    ///
    /// The version opnum in the pointers gets reset once this structure drops, e.g. all updates have been flushed.
    history: SmallVec<[VersionedPointer; 1]>,
}

impl PointerUpdates {
    /// Set the current latest pointer
    pub(super) fn set(&mut self, pointer: ValuePointer) {
        let opnum = self.history.last().map(|p| p.opnum + 1).unwrap_or(0);
        let pointer = VersionedPointer { opnum, pointer };
        self.history.push(pointer);
        self.latest_is_set = true;
    }

    /// Mark this pointer as pending for freeing
    pub(super) fn unset(&mut self, pointer: ValuePointer) {
        let opnum = self.history.last().map(|p| p.opnum + 1).unwrap_or(0);
        match self.history.last_mut() {
            // Prevent duplicating pointers to free, but still update the opnum
            Some(last) if last.pointer == pointer => last.opnum = opnum,
            // If it is a different pointer, or it is the first one, add to history
            _ => self.history.push(VersionedPointer { opnum, pointer }),
        }
        self.latest_is_set = false;
    }

    /// Set is Some, Unset is None
    pub(crate) fn latest(&self) -> Option<ValuePointer> {
        if self.latest_is_set {
            self.history
                .last()
                .map(|versioned| &versioned.pointer)
                .copied()
        } else {
            None
        }
    }

    /// Returns pointers that need to be freed, i.e. They have been written, and are no longer needed
    pub(crate) fn to_outdated_pointers(&self) -> impl Iterator<Item = ValuePointer> {
        let take = if self.latest_is_set {
            // all but the latest one
            self.history.len().saturating_sub(1)
        } else {
            // all of them
            self.history.len()
        };

        self.history
            .iter()
            .map(|versioned| &versioned.pointer)
            .copied()
            .take(take)
    }

    /// Bump this pointer updates structure to drain all details that have been persisted
    ///
    /// The pointer updates structure that we have persisted must be given. All persisted details
    /// that are inside the current pointer updates structure are removed in-place. The pointer
    /// updates structure we're left with only contains details that have not yet been persisted.
    ///
    /// Returns Some(edited_self) if there are newer changes than persisted.
    #[must_use = "if Some is returned, it must be reinserted into the map"]
    pub(super) fn drain_persisted(mut self, persisted: &Self) -> Option<Self> {
        // We don't expect duplicate pointers in history
        debug_assert!(
            !self.history.is_empty(),
            "self must not have empty pointer history",
        );
        debug_assert!(
            !persisted.history.is_empty(),
            "persisted must not have empty pointer history",
        );
        debug_assert_eq!(
            self.history.iter().copied().collect::<AHashSet<_>>().len(),
            self.history.len(),
            "self must not have duplicate pointers in history",
        );
        debug_assert_eq!(
            persisted
                .history
                .iter()
                .copied()
                .collect::<AHashSet<_>>()
                .len(),
            persisted.history.len(),
            "persisted must not have duplicate pointers in history",
        );

        // If both are the same, we have persisted the entry and can drop the pending change
        if &self == persisted {
            return None;
        }

        let max_persisted_opnum = persisted.history.iter().map(|p| p.opnum).max().unwrap_or(0);

        // Remove all persisted pointers from history
        self.history
            .retain(|pointer| pointer.opnum > max_persisted_opnum);

        // Drop entry if history is exhausted
        if self.history.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use smallvec::SmallVec;

    use crate::pointer_updates::{PointerUpdates, VersionedPointer};
    use crate::tracker::ValuePointer;

    /// Test pointer drain edge case that was previously broken.
    ///
    /// See: <https://github.com/qdrant/qdrant/pull/7741>
    #[test]
    fn test_value_pointer_drain_bug_7741() {
        // current:
        // - latest: true
        // - history: [block_offset:1, block_offset:2]
        //
        // persisted:
        // - latest: false
        // - history: [block_offset:1]
        //
        // expected current after drain:
        // - latest: true
        // - history: [block_offset:2]

        let mut updates = PointerUpdates::default();

        // Put and delete block offset 1
        updates.set(ValuePointer::new(1, 1, 1));
        updates.unset(ValuePointer::new(1, 1, 1));

        // Clone this set of updates to flush later
        let persisted = updates.clone();

        // Put block offset 2
        updates.set(ValuePointer::new(1, 2, 1));

        // Drain persisted updates and don't drop, still need to persist block offset 2 later
        let Some(updates) = updates.drain_persisted(&persisted) else {
            panic!("Expected Some(updates)");
        };

        // Pending updates must only have set for block offset 2
        let expected = PointerUpdates {
            latest_is_set: true,
            history: SmallVec::from([VersionedPointer::new_raw(2, 1, 2, 1)]),
        };

        assert_eq!(
            updates, expected,
            "must have one pending update to set block offset 2",
        );
    }

    #[test]
    fn test_value_pointer_drain() {
        let mut updates = PointerUpdates::default();
        updates.set(ValuePointer::new(1, 1, 1));

        // When all updates are persisted, drop the entry
        assert!(
            updates.clone().drain_persisted(&updates).is_none(),
            "must drop entry"
        );

        updates.set(ValuePointer::new(1, 2, 1));

        // When all updates are persisted, drop the entry
        assert!(
            updates.clone().drain_persisted(&updates).is_none(),
            "must drop entry"
        );

        let persisted = updates.clone();
        updates.set(ValuePointer::new(1, 3, 1));

        // Last pointer was not persisted, only keep it for the next flush
        {
            let updates = updates.clone();
            let Some(updates) = updates.drain_persisted(&persisted) else {
                panic!("Expected Some(updates)");
            };
            assert!(updates.latest_is_set);
            assert_eq!(
                updates.history.as_slice(),
                &[VersionedPointer::new_raw(2, 1, 3, 1)]
            );
        }

        updates.set(ValuePointer::new(1, 4, 1));

        // Last two pointers were not persisted, only keep them for the next flush
        {
            let updates = updates.clone();
            let Some(updates) = updates.drain_persisted(&persisted) else {
                panic!("Expected Some(updates)");
            };
            assert!(updates.latest_is_set);
            assert_eq!(
                updates.history.as_slice(),
                &[
                    VersionedPointer::new_raw(2, 1, 3, 1),
                    VersionedPointer::new_raw(3, 1, 4, 1)
                ]
            );
        }

        let persisted = updates.clone();
        updates.unset(ValuePointer::new(1, 4, 1));

        // Last pointer write is persisted, but the delete of the last pointer is not
        // Then we keep the last pointer with set=false to flush the delete next time
        {
            let updates = updates.clone();
            let Some(updates) = updates.drain_persisted(&persisted) else {
                panic!("Expected Some(updates)");
            };
            assert!(!updates.latest_is_set);
            assert_eq!(
                updates.history.as_slice(),
                &[VersionedPointer::new_raw(4, 1, 4, 1)]
            );
        }

        // Even if the history would somehow be shuffled we'd still drain properly
        {
            let updates = updates.clone();
            let mut persisted = updates.clone();
            persisted.history.swap(0, 1);
            persisted.history.swap(1, 3);
            assert!(updates.drain_persisted(&persisted).is_none());
        }
    }
}
