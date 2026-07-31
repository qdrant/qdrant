//! Global resource quotas.
//!
//! Memory and disk are node-wide resources, so their limits are configured once
//! for the whole cluster rather than per collection. [`QuotaManager`] owns that
//! configuration: it is seeded from the storage settings (and thus from config
//! files and environment variables), overridden by a quota file persisted at the
//! root of the storage directory, and updated cluster-wide through consensus.
//! `storage` re-exports this module and drives all of that.
//!
//! It is also the single place that measures those resources, for its own limit
//! checks and for anything else that needs to know how full a disk is — the
//! optimizer sizing up a merge, the WAL checking it has room to write. Nothing
//! else calls `statvfs` or reads process memory.
//!
//! Code with limits of its own — a collection's strict mode config — passes them
//! in as [`QuotaLimits`] overrides. An override can only tighten, so no
//! per-collection setting can lift a cluster-wide limit.

mod check;
mod config;
mod error;
mod manager;
mod meter;
mod status;

use std::sync::{Arc, OnceLock};

pub use config::{QuotaConfig, QuotaLimits};
pub use error::{QuotaError, QuotaResult};
pub use manager::{DiskFit, QUOTA_CONFIG_FILE, QuotaManager};
pub use status::{QuotaStatus, QuotaUsage};

static GLOBAL: OnceLock<Arc<QuotaManager>> = OnceLock::new();

/// Install the node's quota manager, once at startup before anything measures a
/// resource. `storage` does it while building the table of contents.
///
/// A second call is a startup-order bug — the quota it configures would silently
/// not be the one enforced — so it is loud rather than ignored.
pub fn set_global(manager: Arc<QuotaManager>) {
    if GLOBAL.set(manager).is_err() {
        log::error!(
            "Global quota manager was already initialized; \
             the quota configured for this node is not the one being enforced",
        );
        debug_assert!(false, "global quota manager initialized twice");
    }
}

/// The node's quota manager. Falls back to one that enforces nothing for
/// binaries with no quota config to install — edge, and tests — whose
/// measurements still work, which is all a non-enforcing caller needs.
pub fn global() -> &'static Arc<QuotaManager> {
    GLOBAL.get_or_init(|| Arc::new(QuotaManager::default()))
}
