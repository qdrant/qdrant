//! Global resource quotas.
//!
//! Memory and disk are node-wide resources, so their limits are configured once
//! for the whole cluster rather than per collection. [`QuotaManager`] owns that
//! configuration: it is seeded from [`crate::types::StorageConfig`] (and thus
//! from config files and environment variables), overridden by a quota file
//! persisted at the root of the storage directory, and updated cluster-wide
//! through consensus.

mod check;
mod config;
mod manager;
mod status;

pub use check::{EffectiveLimit, LimitSource};
pub use config::{QuotaConfig, QuotaLimits};
pub use manager::{QUOTA_CONFIG_FILE, QuotaManager};
pub use status::{QuotaStatus, QuotaUsage};
