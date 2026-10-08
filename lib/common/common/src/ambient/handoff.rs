use super::{AmbientContext, Scope, slot};
use crate::cpu_utilization::CpuUtilization;
use crate::reason::Reason;

/// The context of the current scope, to enter it on another thread or task.
pub fn current() -> Handoff {
    Handoff(slot::current_ctx())
}

/// Run rayon (or any other work-stealing) calls.
/// Closures passed to rayon must enter the provided context, see [`Handoff::enter`].
pub fn parallel<R>(f: impl FnOnce(&Handoff) -> R) -> R {
    let ctx = current();
    let _scope = slot::enter_masked();
    f(&ctx)
}

/// Run `f` without measuring it.
pub fn unmeasured<R>(_: Reason, f: impl FnOnce() -> R) -> R {
    let _scope = slot::enter_unmeasured();
    f()
}

/// Guard version of [`unmeasured`]. Don't hold it across `.await`.
pub fn unmeasured_guard(_: Reason) -> Scope<'static> {
    slot::enter_unmeasured()
}

#[cfg(any(test, feature = "testing"))]
pub fn test<R>(f: impl FnOnce() -> R) -> R {
    unmeasured(crate::reason::reason("Test code"), f)
}

#[cfg(any(test, feature = "testing"))]
pub fn test_guard() -> Scope<'static> {
    unmeasured_guard(crate::reason::reason("Test code"))
}

#[derive(Clone, Debug)]
pub struct Handoff(Option<AmbientContext>);

impl Handoff {
    pub fn measured(ctx: AmbientContext) -> Self {
        Self(Some(ctx))
    }

    /// Don't measure. Use for internal operations, which are not attributed to any request.
    pub fn unmeasured(_: Reason) -> Self {
        Self(None)
    }

    /// Run `f` in this context.
    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        let _scope = self.enter_guard();
        f()
    }

    /// Guard version of [`Self::enter`]. Don't hold it across `.await`.
    pub fn enter_guard(&self) -> Scope<'_> {
        match &self.0 {
            Some(ctx) => slot::enter_measured(ctx),
            None => slot::enter_unmeasured(),
        }
    }

    pub fn is_measured(&self) -> bool {
        self.0.is_some()
    }

    pub fn context(&self) -> Option<&AmbientContext> {
        self.0.as_ref()
    }

    /// CPU utilization of the context; a fresh one when unmeasured.
    pub fn cpu_utilization(&self) -> CpuUtilization {
        self.0
            .as_ref()
            .map_or_else(CpuUtilization::new, AmbientContext::cpu_utilization)
    }
}
