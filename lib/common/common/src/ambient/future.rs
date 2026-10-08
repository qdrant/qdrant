use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use super::{AmbientContext, HwHandoff, current};
use crate::reason::Reason;

/// Future adapters that enter a scope on every poll.
pub trait HwFutureExt: Future + Sized {
    fn measured(self, ctx: AmbientContext) -> HwFuture<Self> {
        self.in_hw(HwHandoff::measured(ctx))
    }

    fn unmeasured(self, reason: Reason) -> HwFuture<Self> {
        self.in_hw(HwHandoff::unmeasured(reason))
    }

    /// Enter a context taken from [`current`].
    fn in_hw(self, hw: HwHandoff) -> HwFuture<Self> {
        HwFuture { hw, future: self }
    }

    fn in_current_hw(self) -> HwFuture<Self> {
        self.in_hw(current())
    }
}

impl<F: Future> HwFutureExt for F {}

pin_project! {
    pub struct HwFuture<F> {
        hw: HwHandoff,
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for HwFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let _scope = this.hw.enter_guard();
        this.future.poll(cx)
    }
}
