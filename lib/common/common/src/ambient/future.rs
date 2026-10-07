use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use super::{AmbientContext, Handoff, current, slot};
use crate::reason::Reason;

/// Future adapters that enter a scope on every poll.
pub trait AmbientFutureExt: Future + Sized {
    fn measured(self, ctx: AmbientContext) -> AmbientFuture<Self> {
        self.in_ambient(Handoff::Measured(ctx))
    }

    fn unmeasured(self, reason: Reason) -> AmbientFuture<Self> {
        self.in_ambient(Handoff::unmeasured(reason))
    }

    fn in_ambient(self, handoff: Handoff) -> AmbientFuture<Self> {
        AmbientFuture {
            handoff,
            future: self,
        }
    }

    fn in_current_ambient(self) -> AmbientFuture<Self> {
        self.in_ambient(current())
    }
}

impl<F: Future> AmbientFutureExt for F {}

pin_project! {
    pub struct AmbientFuture<F> {
        handoff: Handoff,
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for AmbientFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        slot::enter(this.handoff, || this.future.poll(cx))
    }
}
