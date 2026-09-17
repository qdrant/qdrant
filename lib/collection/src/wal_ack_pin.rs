use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

/// The set of live WAL acknowledge pins of a single shard.
///
/// A pin holds back acknowledging - and thereby truncating - the WAL from a given version onwards,
/// for code that still needs to read those operations back. The queue proxy shard is such a user:
/// it replays operations from the WAL to catch up a remote shard.
///
/// Any number of pins may be live at the same time, they never clobber each other. The WAL is
/// never acknowledged past the lowest of them. If there are no pins, everything that is confirmed
/// to be flushed is acknowledged.
///
/// # Lock order
///
/// Installing a pin and acknowledging the WAL must be serialized against each other, otherwise a
/// pin installed between reading the pins and acknowledging is truncated away regardless. Both
/// sides therefore take the WAL lock first and the pin lock second, never the other way around:
///
/// - [`QueueProxyShard`] creation holds the WAL lock across [`WalAckPins::pin`]
/// - the flush worker holds the WAL lock across [`WalAckPins::max_ack`] and the acknowledge
///
/// Never take the pin lock and then wait for the WAL lock.
///
/// [`QueueProxyShard`]: crate::shards::queue_proxy_shard::QueueProxyShard
#[derive(Default)]
pub struct WalAckPins {
    /// One entry per [`WalAckPinGuard`] handed out, holding the version that pin keeps.
    ///
    /// Held weakly, so that dropping a [`WalAckPinGuard`] is all it takes to release its pin.
    /// Entries of released pins are pruned on the next access.
    pins: Mutex<Vec<Weak<AtomicU64>>>,
}

impl WalAckPins {
    /// Pin the WAL acknowledge at `version`.
    ///
    /// Operations from `version` onwards are not acknowledged, and therefore not truncated from
    /// the WAL, for as long as the returned [`WalAckPinGuard`] is alive.
    pub fn pin(&self, version: u64) -> WalAckPinGuard {
        log::trace!("Pinning WAL acknowledge at {version}");
        let guard = WalAckPinGuard {
            version: Arc::new(AtomicU64::new(version)),
        };

        let mut pins = self.pins.lock();
        pins.retain(|pin| pin.strong_count() > 0);
        pins.push(Arc::downgrade(&guard.version));

        guard
    }

    /// The version to acknowledge the WAL at, given everything up to `confirmed` is flushed.
    ///
    /// [`SerdeWal::ack`] is exclusive: it keeps the entry at the version it is given and drops
    /// only what is strictly before it. Acknowledging at the lowest pin is therefore exactly what
    /// keeps that pinned entry, no `- 1` needed.
    ///
    /// Must be called with the WAL lock held, see the lock order on [`WalAckPins`].
    ///
    /// [`SerdeWal::ack`]: shard::wal::SerdeWal::ack
    pub fn max_ack(&self, confirmed: u64) -> u64 {
        match self.lowest() {
            Some(lowest_pin) => confirmed.min(lowest_pin),
            None => confirmed,
        }
    }

    /// The lowest version pinned by any live [`WalAckPinGuard`], `None` if there are no pins.
    ///
    /// The WAL must not be acknowledged past this version.
    pub fn lowest(&self) -> Option<u64> {
        let mut pins = self.pins.lock();
        pins.retain(|pin| pin.strong_count() > 0);
        pins.iter()
            .filter_map(Weak::upgrade)
            .map(|version| version.load(Ordering::Relaxed))
            .min()
    }
}

impl fmt::Debug for WalAckPins {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pinned: Vec<_> = self
            .pins
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|version| version.load(Ordering::Relaxed))
            .collect();
        f.debug_struct("WalAckPins")
            .field("pinned", &pinned)
            .finish()
    }
}

/// A live pin on the WAL acknowledge, see [`WalAckPins::pin`].
///
/// Holds back the WAL acknowledge at the pinned version until this is dropped.
#[must_use = "a WAL acknowledge pin is released the moment it is dropped"]
pub struct WalAckPinGuard {
    /// The version this pin keeps.
    ///
    /// [`WalAckPins`] only holds a [`Weak`] reference to it, so dropping this releases the pin.
    version: Arc<AtomicU64>,
}

impl WalAckPinGuard {
    /// Move this pin to `version`.
    ///
    /// Use this to release WAL entries the holder no longer needs, while keeping the pin itself.
    ///
    /// This is not expected to move the version backwards, gated by a debug assertion.
    pub fn set(&self, version: u64) {
        log::trace!("Moving WAL acknowledge pin to {version}");
        let previous = self.version.swap(version, Ordering::Relaxed);

        debug_assert!(
            version >= previous,
            "WAL acknowledge pin moved backwards from {previous} to {version}",
        );
    }
}

impl fmt::Debug for WalAckPinGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WalAckPinGuard")
            .field(&self.version.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_without_pins() {
        let pins = WalAckPins::default();
        assert_eq!(pins.lowest(), None);
    }

    #[test]
    fn test_pin_releases_on_drop() {
        let pins = WalAckPins::default();

        let pin = pins.pin(10);
        assert_eq!(pins.lowest(), Some(10));

        drop(pin);
        assert_eq!(pins.lowest(), None);
    }

    #[test]
    fn test_lowest_of_multiple_pins() {
        let pins = WalAckPins::default();

        let high = pins.pin(30);
        let low = pins.pin(10);
        let middle = pins.pin(20);
        assert_eq!(pins.lowest(), Some(10));

        // Releasing the lowest pin moves the bound up to the next one
        drop(low);
        assert_eq!(pins.lowest(), Some(20));

        // Releasing a pin that isn't the lowest doesn't move the bound
        drop(high);
        assert_eq!(pins.lowest(), Some(20));

        drop(middle);
        assert_eq!(pins.lowest(), None);
    }

    #[test]
    fn test_move_pin_forward() {
        let pins = WalAckPins::default();

        let first = pins.pin(10);
        let second = pins.pin(20);
        assert_eq!(pins.lowest(), Some(10));

        // Moving the lowest pin past the other makes that other one the lowest
        first.set(25);
        assert_eq!(pins.lowest(), Some(20));

        // A pin may also stay
        second.set(20);
        assert_eq!(pins.lowest(), Some(20));
    }

    #[test]
    #[should_panic]
    fn test_move_pin_backward() {
        let pins = WalAckPins::default();
        let pin = pins.pin(10);

        // A pin should not move back
        pin.set(5);
    }

    #[test]
    fn test_max_ack_without_pins() {
        let pins = WalAckPins::default();

        // Everything confirmed flushed may be acknowledged
        assert_eq!(pins.max_ack(0), 0);
        assert_eq!(pins.max_ack(100), 100);
        assert_eq!(pins.max_ack(u64::MAX), u64::MAX);
    }

    #[test]
    fn test_max_ack_is_capped_by_the_lowest_pin() {
        let pins = WalAckPins::default();
        let _pin = pins.pin(10);

        // A pin ahead of what is confirmed doesn't hold anything back
        assert_eq!(pins.max_ack(5), 5);

        // `SerdeWal::ack` is exclusive, so acknowledging *at* the pin keeps the pinned entry
        assert_eq!(pins.max_ack(10), 10);
        assert_eq!(pins.max_ack(100), 10);
    }

    /// A pin at the very first WAL index must not stop the flush pass, it just caps the
    /// acknowledge at 0, which truncates nothing.
    ///
    /// Regression test: this used to be a special case that abandoned the rest of the flush pass,
    /// taking clock map persistence with it for the lifetime of the pin.
    #[test]
    fn test_max_ack_with_pin_at_zero() {
        let pins = WalAckPins::default();
        let _pin = pins.pin(0);

        assert_eq!(pins.max_ack(0), 0);
        assert_eq!(pins.max_ack(100), 0);
    }

    #[test]
    fn test_max_ack_follows_pins_as_they_move_and_release() {
        let pins = WalAckPins::default();

        let first = pins.pin(10);
        let second = pins.pin(20);
        assert_eq!(pins.max_ack(100), 10);

        // Moving the lowest pin forward releases WAL up to the next one
        first.set(30);
        assert_eq!(pins.max_ack(100), 20);

        // Releasing the last pin lifts the cap entirely
        drop(second);
        drop(first);
        assert_eq!(pins.max_ack(100), 100);
    }

    #[test]
    fn test_released_pins_are_pruned() {
        let pins = WalAckPins::default();

        // Keep one pin alive so the list is never empty and can only be pruned selectively
        let _pin = pins.pin(1);

        for _ in 0..100 {
            let _ = pins.pin(2);
            assert_eq!(pins.lowest(), Some(1));
        }

        assert_eq!(pins.pins.lock().len(), 1);
    }
}
