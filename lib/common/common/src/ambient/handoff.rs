use ecow::EcoString;

use super::{AmbientContext, Scope, slot};
use crate::cpu_utilization::CpuUtilization;
use crate::reason::Reason;

/// The current scope, to enter it on another thread or task.
#[cfg_attr(debug_assertions, track_caller)]
pub fn current() -> Handoff {
    Handoff::current()
}

/// Run rayon (or any other work-stealing) calls.
/// Closures passed to rayon must enter the provided scope, see [`Handoff::enter`].
pub fn parallel<R>(f: impl FnOnce(&Handoff) -> R) -> R {
    let handoff = current();
    let _scope = Scope::masked();
    f(&handoff)
}

/// Run `f` without measuring it.
pub fn unmeasured<R>(reason: Reason, f: impl FnOnce() -> R) -> R {
    let _scope = unmeasured_guard(reason);
    f()
}

/// Guard version of [`unmeasured`]. Don't hold it across `.await`.
pub fn unmeasured_guard(_: Reason) -> Scope {
    Scope::unmeasured()
}

#[cfg(any(test, feature = "testing"))]
pub fn test<R>(f: impl FnOnce() -> R) -> R {
    unmeasured(crate::reason::reason("Test code"), f)
}

#[cfg(any(test, feature = "testing"))]
pub fn test_guard() -> Scope {
    unmeasured_guard(crate::reason::reason("Test code"))
}

/// A scope that can be entered elsewhere.
#[derive(Clone, Debug)]
pub enum Handoff {
    /// Not measured; still traced into the span, if any.
    Unmeasured(Option<AmbientContext>),
    Measured(AmbientContext),
}

impl Handoff {
    /// Don't measure, but stay in the current span.
    /// Use for internal operations, which are not attributed to any request.
    pub fn unmeasured(_: Reason) -> Self {
        Self::Unmeasured(Self::current_unchecked().into_context())
    }

    /// Run `f` in this scope.
    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        slot::enter(self, f)
    }

    /// Guard version of [`Self::enter`]. Don't hold it across `.await`.
    pub fn enter_guard(&self) -> Scope {
        self.clone().into_scope()
    }

    /// [`Self::enter_guard`] without the clone.
    pub fn into_scope(self) -> Scope {
        Scope::enter_owned(self)
    }

    /// This scope, moved to a new child span, see [`super::trace::span!`].
    pub fn span(self, name: impl FnOnce() -> EcoString) -> Self {
        self.map_context(|ctx| {
            if ctx.is_traced() {
                ctx.child(name)
            } else {
                ctx
            }
        })
    }

    /// The context, measured or not.
    pub fn context(&self) -> Option<&AmbientContext> {
        match self {
            Self::Unmeasured(ctx) => ctx.as_ref(),
            Self::Measured(ctx) => Some(ctx),
        }
    }

    pub fn into_context(self) -> Option<AmbientContext> {
        match self {
            Self::Unmeasured(ctx) => ctx,
            Self::Measured(ctx) => Some(ctx),
        }
    }

    /// The context, if measured.
    pub fn measured(&self) -> Option<&AmbientContext> {
        match self {
            Self::Unmeasured(_) => None,
            Self::Measured(ctx) => Some(ctx),
        }
    }

    /// CPU utilization of the context; a fresh one when unmeasured.
    pub fn cpu_utilization(&self) -> CpuUtilization {
        self.measured()
            .map_or_else(CpuUtilization::new, AmbientContext::cpu_utilization)
    }

    fn map_context(self, f: impl FnOnce(AmbientContext) -> AmbientContext) -> Self {
        match self {
            Self::Unmeasured(ctx) => Self::Unmeasured(ctx.map(f)),
            Self::Measured(ctx) => Self::Measured(f(ctx)),
        }
    }
}
