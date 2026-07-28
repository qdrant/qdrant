//! Node-wide selection of the `io_uring` universal-IO backend for segment components.
//!
//! A few components have both an mmap and an io_uring variant reading the very same files: the
//! immutable dense vector storage, the single-file TurboQuant vector storage, and the mmap
//! payload storage. Which variant is opened is a node-wide decision, stored here once at
//! startup from `storage.performance.io_uring` and read back by each component's constructor
//! through [`use_io_uring`].

use std::sync::atomic::{AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

use crate::types::Memory;
use crate::vector_storage::common::get_async_scorer;

/// How this node picks between the mmap and io_uring backends of a component that has both.
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IoUringMode {
    /// Never open a component on io_uring, whatever else is configured.
    Disabled,
    /// Open a component on io_uring where it can pay off and is possible: the component keeps
    /// its data cold (there is nothing to gain from io_uring for data meant to sit in the page
    /// cache, where mmap is the faster of the two), its feature flag allows it, and the kernel
    /// supports io_uring.
    Auto,
}

/// What a component falls back to when the node-wide setting is absent.
///
/// The setting is optional, and an absent setting must not change how an existing deployment
/// behaves — which is not the same answer for every component, hence this choice.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IoUringFallback {
    /// Follow the legacy `storage.performance.async_scorer` setting. For the vector storages,
    /// whose io_uring variants have always shipped under that setting.
    AsyncScorer,
    /// Stay on mmap. For the payload storage, whose io_uring variant is only ever reachable
    /// through the `io_uring` setting.
    Mmap,
}

/// Node-wide [`IoUringMode`], encoded by [`encode_mode`] since there is no atomic enum.
static IO_URING_MODE: AtomicU8 = AtomicU8::new(MODE_UNSET);

const MODE_UNSET: u8 = 0;
const MODE_DISABLED: u8 = 1;
const MODE_AUTO: u8 = 2;

fn encode_mode(mode: Option<IoUringMode>) -> u8 {
    match mode {
        None => MODE_UNSET,
        Some(IoUringMode::Disabled) => MODE_DISABLED,
        Some(IoUringMode::Auto) => MODE_AUTO,
    }
}

fn decode_mode(encoded: u8) -> Option<IoUringMode> {
    match encoded {
        MODE_UNSET => None,
        MODE_DISABLED => Some(IoUringMode::Disabled),
        MODE_AUTO => Some(IoUringMode::Auto),
        // Unreachable: `encode_mode` is the only writer of the global.
        _ => {
            debug_assert!(false, "unknown encoded io_uring mode: {encoded}");
            None
        }
    }
}

/// Set the node-wide io_uring mode.
///
/// Call once at startup, before any segment is opened: components read the mode when they are
/// constructed, so a later change only reaches segments opened after it.
pub fn set_io_uring_mode(mode: Option<IoUringMode>) {
    IO_URING_MODE.store(encode_mode(mode), Ordering::Relaxed);
}

/// The node-wide io_uring mode, `None` when the setting is absent from the config.
pub fn io_uring_mode() -> Option<IoUringMode> {
    decode_mode(IO_URING_MODE.load(Ordering::Relaxed))
}

/// Whether a component should be opened on the io_uring backend rather than on mmap.
///
/// `fallback` is what this component did before the `io_uring` setting existed, `memory` is the
/// placement its configured storage type provides, and `feature_flag` is the component's own
/// feature-flag gate (`true` for components that have none). Always `false` off Linux, where
/// there is no io_uring backend to open.
pub fn use_io_uring(fallback: IoUringFallback, memory: Memory, feature_flag: bool) -> bool {
    if !feature_flag {
        return false;
    }

    match io_uring_mode() {
        None => match fallback {
            IoUringFallback::AsyncScorer => get_async_scorer(),
            IoUringFallback::Mmap => false,
        },
        Some(IoUringMode::Disabled) => false,
        Some(IoUringMode::Auto) => memory.is_cold() && is_io_uring_supported(),
    }
}

#[cfg(target_os = "linux")]
fn is_io_uring_supported() -> bool {
    common::universal_io::is_io_uring_supported()
}

#[cfg(not(target_os = "linux"))]
fn is_io_uring_supported() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_storage::common::set_async_scorer;

    /// Both globals are process-wide, so the cases that touch them run in one test to keep
    /// them off other tests' path.
    #[test]
    fn test_use_io_uring() {
        let supported = is_io_uring_supported();

        // Unset: the vector storages follow the async scorer, the payload storage does not.
        set_io_uring_mode(None);
        set_async_scorer(true);
        assert!(use_io_uring(
            IoUringFallback::AsyncScorer,
            Memory::Cold,
            true
        ));
        assert!(use_io_uring(
            IoUringFallback::AsyncScorer,
            Memory::Cached,
            true
        ));
        assert!(!use_io_uring(IoUringFallback::Mmap, Memory::Cold, true));

        set_async_scorer(false);
        assert!(!use_io_uring(
            IoUringFallback::AsyncScorer,
            Memory::Cold,
            true
        ));

        // Disabled: never, whatever the async scorer says.
        set_io_uring_mode(Some(IoUringMode::Disabled));
        set_async_scorer(true);
        assert!(!use_io_uring(
            IoUringFallback::AsyncScorer,
            Memory::Cold,
            true
        ));
        assert!(!use_io_uring(IoUringFallback::Mmap, Memory::Cold, true));

        // Auto: cold placement only, async scorer no longer consulted.
        set_io_uring_mode(Some(IoUringMode::Auto));
        set_async_scorer(false);
        assert_eq!(
            use_io_uring(IoUringFallback::Mmap, Memory::Cold, true),
            supported,
        );
        assert_eq!(
            use_io_uring(IoUringFallback::AsyncScorer, Memory::Cold, true),
            supported,
        );
        assert!(!use_io_uring(IoUringFallback::Mmap, Memory::Cached, true));
        assert!(!use_io_uring(IoUringFallback::Mmap, Memory::Pinned, true));

        // A disabled feature flag vetoes every mode.
        assert!(!use_io_uring(IoUringFallback::Mmap, Memory::Cold, false));

        set_io_uring_mode(None);
        set_async_scorer(false);
    }

    #[test]
    fn test_mode_round_trip() {
        for mode in [None, Some(IoUringMode::Disabled), Some(IoUringMode::Auto)] {
            assert_eq!(decode_mode(encode_mode(mode)), mode);
        }
    }
}
