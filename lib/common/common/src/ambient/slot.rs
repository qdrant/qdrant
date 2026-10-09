//! Unsafe/thread-local implementation details for the [`super`] module.

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;

use strum::EnumCount;

use super::context::{AmbientContext, Root};
use super::hw::{HardwareData, HwMetric};

#[inline]
pub(super) fn bump(metric: HwMetric, delta: usize) {
    SLOT.with(|slot| {
        #[cfg(debug_assertions)]
        slot.check_bump(metric);
        let counter = &slot.counters[metric as usize];
        counter.set(counter.get().wrapping_add(delta));
    });
}

/// The not-yet-flushed value of one counter of the current scope.
pub(super) fn pending_metric(metric: HwMetric) -> usize {
    SLOT.with(|slot| slot.counters[metric as usize].get())
}

/// The not-yet-flushed values of all counters of the current scope.
#[cfg(any(test, feature = "testing"))]
pub(super) fn pending() -> HardwareData {
    SLOT.with(|slot| HardwareData(slot.counters.each_ref().map(Cell::get)))
}

pub(super) fn enter_measured(ctx: &AmbientContext) -> Scope<'_> {
    Scope::enter(Target::Measured(ctx.as_ptr()), None)
}

pub(super) fn enter_measured_owned(ctx: AmbientContext) -> Scope<'static> {
    let target = Target::Measured(ctx.as_ptr());
    Scope::enter(target, Some(ctx))
}

pub(super) fn enter_unmeasured() -> Scope<'static> {
    Scope::enter(Target::Unmeasured, None)
}

/// Mask the current scope, see [`super::parallel`].
pub(super) fn enter_masked() -> Scope<'static> {
    Scope::enter(Target::Unset, None)
}

pub(super) fn current_ctx() -> Option<AmbientContext> {
    SLOT.with(|slot| {
        #[cfg(debug_assertions)]
        slot.check_access("ambient::current()");
        match slot.target.get() {
            // SAFETY: the pointer belongs to the innermost scope, which keeps it alive.
            Target::Measured(node) => Some(unsafe { AmbientContext::from_ptr(node) }),
            Target::Unset | Target::Unmeasured => None,
        }
    })
}

pub(super) fn is_measured() -> bool {
    SLOT.with(|slot| {
        #[cfg(debug_assertions)]
        slot.check_access("hw::is_measured()");
        matches!(slot.target.get(), Target::Measured(_))
    })
}

/// [`AmbientContext::accumulate_request`] on the current context, if measured.
pub(super) fn accumulate_request(src: HardwareData) {
    SLOT.with(|slot| {
        if let Target::Measured(node) = slot.target.get() {
            // SAFETY: the pointer belongs to the innermost scope, which keeps it alive.
            unsafe { node.as_ref() }.accumulate_request(src);
        }
    });
}

thread_local! {
    static SLOT: Slot = const {
        Slot {
            target: Cell::new(Target::Unset),
            depth: Cell::new(0),
            counters: [const { Cell::new(0) }; HwMetric::COUNT],
        }
    };
}

struct Slot {
    target: Cell<Target>,
    /// Number of active scopes, to check they are exited in reverse order.
    depth: Cell<u64>,
    counters: [Cell<usize>; HwMetric::COUNT],
}

impl Slot {
    #[cfg(debug_assertions)]
    #[inline]
    fn check_bump(&self, metric: HwMetric) {
        if self.target.get() == Target::Unset {
            self.check_access(metric);
        }
    }

    #[cfg(debug_assertions)]
    #[cold]
    fn check_access(&self, what: impl std::fmt::Debug) {
        if self.target.get() == Target::Unset {
            panic!(
                "{what:?} outside of any ambient scope: a spawned/stolen job forgot to enter its \
                 context, or code inside ambient::parallel() didn't enter the provided one",
            );
        }
    }

    /// Drop the measurements and make every active scope stale.
    #[cold]
    fn poison(&self) {
        self.depth.set(self.depth.get() + (1 << 32));
        self.target.set(Target::Unmeasured);
        self.counters.iter().for_each(|c| c.set(0));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    /// The default state of a thread-local slot.
    /// Bumping counters in this state is a misuse:
    ///   it will panic in debug builds and unmeasured in release builds.
    Unset,
    /// Explicitly unmeasured.
    Unmeasured,
    Measured(NonNull<Root>),
}

/// An active scope. Restores the outer one on drop/panic.
#[must_use]
pub struct Scope<'a> {
    depth: u64,
    /// The target of the outer scope, restored on drop.
    outer_target: Target,
    outer_counters: [usize; HwMetric::COUNT],
    /// Keeps [`Target::Measured`] alive for owned guard scopes.
    _owned: Option<AmbientContext>,
    /// Keeps [`Target::Measured`] alive for borrowed guard scopes.
    _borrow: PhantomData<&'a Root>,
}

#[cfg(test)]
// Make sure that a future holding Scope across `.await` can't be spawned on a
// multi-threaded runtime.
static_assertions::assert_not_impl_any!(Scope<'static>: Send);

impl<'a> Scope<'a> {
    fn enter(target: Target, owned: Option<AmbientContext>) -> Self {
        SLOT.with(|slot| Self {
            depth: slot.depth.replace(slot.depth.get() + 1),
            outer_target: slot.target.replace(target),
            outer_counters: std::array::from_fn(|i| slot.counters[i].take()),
            _owned: owned,
            _borrow: PhantomData,
        })
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        SLOT.with(|slot| {
            let in_order = slot.depth.get() == self.depth + 1;
            debug_assert!(
                in_order || std::thread::panicking(),
                "ambient scopes exited out of order"
            );
            if !in_order {
                return slot.poison();
            }
            slot.depth.set(self.depth);
            let counters: [usize; HwMetric::COUNT] =
                std::array::from_fn(|i| slot.counters[i].replace(self.outer_counters[i]));
            let target = slot.target.replace(self.outer_target);
            if let Target::Measured(node) = target
                && counters.iter().any(|&c| c != 0)
            {
                // SAFETY: exited in order, so `node` belongs to this scope, which keeps it alive.
                unsafe { node.as_ref() }.accumulate(HardwareData(counters));
            }
        });
    }
}
