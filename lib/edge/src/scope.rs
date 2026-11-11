use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient::{AmbientContext, Handoff};

#[cfg(feature = "serverless")]
use crate::{HardwareData, HwSharedDrain};

/// Scope to run operations in, see [`EdgeShard::with`].
///
/// [`EdgeShard::with`]: crate::EdgeShard::with
pub struct EdgeScope {
    ctx: AmbientContext,
    is_stopped: Arc<AtomicBool>,
}

impl EdgeScope {
    /// Create a new scope.
    pub fn new() -> Self {
        Self {
            ctx: AmbientContext::root("", None, None),
            is_stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Attach a cancellation flag.
    ///
    /// When the flag is set to `true`, all operations in this scope are
    /// cancelled and return [`OperationError::Cancelled`].
    ///
    /// [`OperationError::Cancelled`]: crate::OperationError::Cancelled
    #[must_use]
    pub fn with_cancellation(mut self, is_stopped: Arc<AtomicBool>) -> Self {
        self.is_stopped = is_stopped;
        self
    }

    /// Attach a drain to collect hardware usage.
    ///
    /// Doesn't affect [`EdgeScope::hw_data()`], which is always collected.
    #[cfg(feature = "serverless")]
    #[must_use]
    pub fn with_drain(mut self, drain: Arc<HwSharedDrain>) -> Self {
        self.ctx = AmbientContext::root("", Some(drain), None);
        self
    }

    /// Get hardware usage for this scope.
    ///
    /// Always collected. Independent of [`EdgeScope::with_drain()`].
    #[cfg(feature = "serverless")]
    pub fn hw_data(&self) -> HardwareData {
        self.ctx.hw_data()
    }

    pub(crate) fn is_stopped(&self) -> &Arc<AtomicBool> {
        &self.is_stopped
    }

    pub(crate) fn handoff(&self) -> Handoff {
        Handoff::Measured(self.ctx.clone())
    }
}

impl Default for EdgeScope {
    fn default() -> Self {
        Self::new()
    }
}

/// A shard paired with the scope its operations run in, see [`EdgeShard::with`].
///
/// [`EdgeShard::with`]: crate::EdgeShard::with
pub struct Scoped<'a, T: ?Sized> {
    pub(crate) shard: &'a T,
    pub(crate) scope: &'a EdgeScope,
}
