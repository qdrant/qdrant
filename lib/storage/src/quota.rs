//! Global resource quotas.
//!
//! The quota itself lives in [`shard::quota`], because the components that have
//! to measure memory and disk — the optimizer, the WAL — sit below this crate
//! and must go through the same [`QuotaManager`] rather than reading the OS
//! themselves. What stays here is everything that makes it *cluster-wide*: the
//! config is seeded from [`crate::types::StorageConfig`], replicated through
//! consensus, and exposed over the `/quotas` API.

pub use shard::quota::{
    QUOTA_CONFIG_FILE, QuotaConfig, QuotaError, QuotaManager, QuotaResult, QuotaStatus, QuotaUsage,
    global, set_global,
};
