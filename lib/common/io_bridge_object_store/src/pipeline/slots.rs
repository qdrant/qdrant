use ahash::AHashMap;

/// Caller-side bookkeeping for in-flight reads, keyed by a monotonic slot id.
///
/// The slot is the only part of a scheduled read that crosses to the worker
/// task — [`UserData`](common::universal_io::UserData) carries no `Send` /
/// `'static` bound, so the real `U` stays here and is reunited with its reply
/// by slot once the task reports back. The destination buffer travels with the
/// future itself and comes back through the reply channel; it is not stored
/// here.
pub(super) struct PendingSlots<U> {
    /// `slot -> user_data` map. Keyed by the slot assigned at [`Self::insert`]
    /// time so out-of-order completion still reunites the response with the
    /// right caller context. `AHashMap` is used over `std::collections::HashMap`
    /// for its faster hashing on small integer keys.
    map: AHashMap<u64, U>,
    /// Monotonic counter that assigns a fresh slot id to every scheduled
    /// request. Wraps around on overflow — collisions only occur if more than
    /// `u64::MAX` requests are simultaneously pending, which is structurally
    /// impossible given the channel capacity.
    next_slot: u64,
}

impl<U> PendingSlots<U> {
    pub(super) fn new() -> Self {
        Self {
            map: AHashMap::new(),
            next_slot: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.map.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Allocate a fresh slot for `user_data` and return the slot id.
    pub(super) fn insert(&mut self, user_data: U) -> u64 {
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.map.insert(slot, user_data);
        slot
    }

    /// Remove and return the `user_data` registered under `slot`, if any.
    pub(super) fn remove(&mut self, slot: u64) -> Option<U> {
        self.map.remove(&slot)
    }
}
