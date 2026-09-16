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
/// never acknowledged at or past the lowest of them. If there are no pins, everything that is
/// confirmed to be flushed is acknowledged.
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

    /// The lowest version pinned by any live [`WalAckPinGuard`], `None` if there are no pins.
    ///
    /// The WAL must not be acknowledged at this version or any later version.
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
    pub fn set(&self, version: u64) {
        log::trace!("Moving WAL acknowledge pin to {version}");
        self.version.store(version, Ordering::Relaxed);
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
    fn test_move_pin() {
        let pins = WalAckPins::default();

        let first = pins.pin(10);
        let second = pins.pin(20);
        assert_eq!(pins.lowest(), Some(10));

        // Moving the lowest pin past the other makes that other one the lowest
        first.set(25);
        assert_eq!(pins.lowest(), Some(20));

        // A pin may also move back
        second.set(5);
        assert_eq!(pins.lowest(), Some(5));
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
