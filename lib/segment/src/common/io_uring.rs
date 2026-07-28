//! Node-wide selection of the `io_uring` universal-IO backend for segment components.
//!
//! Set once at startup from `storage.performance.io_uring`, read back by each component that
//! has both an mmap and an io_uring variant through [`use_io_uring`].

use std::sync::atomic::{AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

use crate::types::Memory;
use crate::vector_storage::common::get_async_scorer;

/// How this node picks between the mmap and io_uring backends of a component that has both.
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IoUringMode {
    /// Never open a component on io_uring.
    Disabled,
    /// Open a component on io_uring when it can pay off and is possible: cold data, feature
    /// flag allows it, kernel supports it. Data meant to sit in the page cache stays on mmap,
    /// which is faster there.
    Auto,
}

/// What a component falls back to when the node-wide setting is absent, so that an absent
/// setting keeps existing deployments behaving as they did. Not the same per component.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IoUringFallback {
    /// Follow `storage.performance.async_scorer`, as the vector storages always have.
    AsyncScorer,
    /// Stay on mmap: reachable only through the `io_uring` setting.
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

/// Set the node-wide io_uring mode. Call before any segment is opened: components read it when
/// constructed, so a later change only reaches segments opened after it.
pub fn set_io_uring_mode(mode: Option<IoUringMode>) {
    IO_URING_MODE.store(encode_mode(mode), Ordering::Relaxed);
}

/// The node-wide io_uring mode, `None` when the setting is absent from the config.
pub fn io_uring_mode() -> Option<IoUringMode> {
    decode_mode(IO_URING_MODE.load(Ordering::Relaxed))
}

/// Whether a component should be opened on io_uring rather than mmap.
///
/// `memory` is the placement its configured storage type provides, and `feature_flag` is the
/// component's own gate (`true` for components that have none). Always `false` off Linux.
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

    /// Both globals are process-wide, so every case runs in one test to keep them off other
    /// tests' path.
    #[test]
    fn test_use_io_uring() {
        use IoUringFallback::{AsyncScorer, Mmap};
        let supported = is_io_uring_supported();

        // Unset: the vector storages follow the async scorer, the payload storage does not.
        set_io_uring_mode(None);
        set_async_scorer(true);
        assert!(use_io_uring(AsyncScorer, Memory::Cold, true));
        assert!(use_io_uring(AsyncScorer, Memory::Cached, true));
        assert!(!use_io_uring(Mmap, Memory::Cold, true));

        set_async_scorer(false);
        assert!(!use_io_uring(AsyncScorer, Memory::Cold, true));

        // Disabled: never, whatever the async scorer says.
        set_io_uring_mode(Some(IoUringMode::Disabled));
        set_async_scorer(true);
        assert!(!use_io_uring(AsyncScorer, Memory::Cold, true));
        assert!(!use_io_uring(Mmap, Memory::Cold, true));

        // Auto: cold placement only, async scorer no longer consulted.
        set_io_uring_mode(Some(IoUringMode::Auto));
        set_async_scorer(false);
        assert_eq!(use_io_uring(Mmap, Memory::Cold, true), supported);
        assert_eq!(use_io_uring(AsyncScorer, Memory::Cold, true), supported);
        assert!(!use_io_uring(Mmap, Memory::Cached, true));
        assert!(!use_io_uring(Mmap, Memory::Pinned, true));

        // A disabled feature flag vetoes every mode.
        assert!(!use_io_uring(Mmap, Memory::Cold, false));

        set_io_uring_mode(None);
        set_async_scorer(false);
    }
}
