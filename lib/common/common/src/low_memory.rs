use std::sync::OnceLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Global low-memory mode, initialized once at startup.
static LOW_MEMORY_MODE: OnceLock<LowMemoryMode> = OnceLock::new();

/// Controls how segments are loaded on startup to reduce memory pressure.
///
/// Configured via `storage.low_memory_mode` in the configuration file.
///
/// This setting only affects *loading* — it never modifies the persisted
/// configuration of any segment. The same data directory will behave
/// differently depending on how this mode is set at startup, which makes it
/// safe to toggle for recovery from out-of-memory crash loops.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LowMemoryMode {
    /// No special handling. Every component loads as persisted.
    #[default]
    Disabled,

    /// Load RAM-friendly components as their on-disk variants where possible:
    ///
    /// * Quantization is loaded as if `always_ram = false`.
    /// * Payload field indexes are loaded as if `on_disk = true`.
    /// * Payload storage is loaded as the mmap variant (lazy populate).
    NoResident,

    /// Same as [`LowMemoryMode::NoResident`], plus mmap page population is
    /// skipped on load (for original vectors, HNSW graph and payload storage).
    NoPopulate,
}

impl LowMemoryMode {
    /// Whether RAM-friendly components should be downgraded to their on-disk
    /// variants at load time.
    pub fn prefer_disk(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Whether `madvise(MADV_POPULATE_READ)` and equivalent prefault should be
    /// skipped when opening mmaps.
    pub fn skip_populate(self) -> bool {
        matches!(self, Self::NoPopulate)
    }
}

/// Decide whether to prefault an mmap on load, for a structure
/// whose `is_on_disk` flag has the given value.
pub fn resolve_populate(is_on_disk: bool) -> bool {
    !is_on_disk && !low_memory_mode().skip_populate()
}

/// Initializes the global low-memory mode. Must only be called once at
/// startup; subsequent calls are ignored with a warning.
pub fn init_low_memory_mode(mode: LowMemoryMode) {
    if LOW_MEMORY_MODE.set(mode).is_err() {
        log::warn!("Low memory mode already initialized!");
    }
}

/// Returns the globally configured low-memory mode.
///
/// Returns [`LowMemoryMode::Disabled`] if the global has not been initialized
/// (e.g. from unit tests). This is intentional: the accessor is called from
/// hot paths like mmap populate, and logging on every call would be noisy.
pub fn low_memory_mode() -> LowMemoryMode {
    LOW_MEMORY_MODE.get().copied().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_disabled() {
        assert_eq!(LowMemoryMode::default(), LowMemoryMode::Disabled);
        assert!(!LowMemoryMode::default().prefer_disk());
        assert!(!LowMemoryMode::default().skip_populate());
    }

    #[test]
    fn test_no_resident_prefers_disk_but_populates() {
        assert!(LowMemoryMode::NoResident.prefer_disk());
        assert!(!LowMemoryMode::NoResident.skip_populate());
    }

    #[test]
    fn test_no_populate_prefers_disk_and_skips_populate() {
        assert!(LowMemoryMode::NoPopulate.prefer_disk());
        assert!(LowMemoryMode::NoPopulate.skip_populate());
    }

    #[test]
    fn test_serde_snake_case() {
        let disabled: LowMemoryMode = serde_json::from_str("\"disabled\"").unwrap();
        assert_eq!(disabled, LowMemoryMode::Disabled);
        let no_resident: LowMemoryMode = serde_json::from_str("\"no_resident\"").unwrap();
        assert_eq!(no_resident, LowMemoryMode::NoResident);
        let no_populate: LowMemoryMode = serde_json::from_str("\"no_populate\"").unwrap();
        assert_eq!(no_populate, LowMemoryMode::NoPopulate);
    }
}
