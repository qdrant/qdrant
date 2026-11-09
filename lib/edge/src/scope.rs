use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient::{AmbientContext, Handoff};

/// Cancellation flag and measurement target of a read.
pub(crate) struct EdgeScope {
    ctx: AmbientContext,
    is_stopped: Arc<AtomicBool>,
}

impl EdgeScope {
    pub(crate) fn new() -> Self {
        Self {
            ctx: AmbientContext::root("", None, None),
            is_stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg_attr(
        not(feature = "serverless"),
        expect(dead_code, reason = "serverless-only caller")
    )]
    pub(crate) fn with_cancellation(mut self, is_stopped: Arc<AtomicBool>) -> Self {
        self.is_stopped = is_stopped;
        self
    }

    pub(crate) fn is_stopped(&self) -> &Arc<AtomicBool> {
        &self.is_stopped
    }

    pub(crate) fn handoff(&self) -> Handoff {
        Handoff::Measured(self.ctx.clone())
    }
}
