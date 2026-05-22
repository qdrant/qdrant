use ahash::AHashMap;

/// Caller-side bookkeeping for in-flight reads, keyed by a monotonic slot id.
///
/// Owns the destination `Vec<T>` for every in-flight request alongside its
/// caller context `U`. The slot is the only part of a scheduled read that
/// crosses to the worker task (the buffer is reached via a raw `SendBytePtr`,
/// and [`UserData`](common::universal_io::UserData) carries no `Send` /
/// `'static` bound), so the real `(U, Vec<T>)` stays here and is reunited with
/// its reply by slot once the task reports back.
pub(super) struct PendingSlots<T, U> {
    /// `slot -> (user_data, destination buffer)` map. The `Vec<T>` is the
    /// buffer the future writes into via its captured `SendBytePtr`. Keyed by
    /// the slot assigned at [`Self::insert`] time so out-of-order completion
    /// still reunites the buffer with the right caller context. `AHashMap` is
    /// used over `std::collections::HashMap` for its faster hashing on small
    /// integer keys.
    map: AHashMap<u64, (U, Vec<T>)>,
    /// Monotonic counter that assigns a fresh slot id to every scheduled
    /// request. Wraps around on overflow — collisions only occur if more than
    /// `u64::MAX` requests are simultaneously pending, which is structurally
    /// impossible given the channel capacity.
    next_slot: u64,
}

impl<T, U> PendingSlots<T, U> {
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

    /// Allocate a fresh slot owning `user_data` and its destination `buf`, and
    /// return the slot id.
    pub(super) fn insert(&mut self, user_data: U, buf: Vec<T>) -> u64 {
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.map.insert(slot, (user_data, buf));
        slot
    }

    /// Remove and return the `(user_data, buffer)` registered under `slot`, if
    /// any.
    pub(super) fn remove(&mut self, slot: u64) -> Option<(U, Vec<T>)> {
        self.map.remove(&slot)
    }
}
