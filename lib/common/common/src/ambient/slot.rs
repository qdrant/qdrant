//! Scary unsafe implementation details for the [`crate::ambient`] module.

use std::cell::Cell;
use std::panic::Location;
use std::ptr::NonNull;

use strum::EnumCount;

use super::Handoff;
use super::context::{AmbientContext, Node};
use super::hw::{HardwareData, HwMetric};

#[inline]
#[cfg_attr(debug_assertions, track_caller)]
pub(super) fn bump(metric: HwMetric, delta: usize) {
    let caller = Location::caller();
    SLOT.with(|slot| {
        _ = slot.target("HwMetric::bump()", caller); // trigger `debug_assertions`
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

/// Run `f` in the scope of `handoff`.
pub(super) fn enter<R>(handoff: &Handoff, f: impl FnOnce() -> R) -> R {
    // SAFETY: `_scope` is dropped before `handoff`, which owns the node.
    let _scope = unsafe { Scope::new(Target::of(handoff), None) };
    f()
}

/// Run `f`, measuring it into `ctx`.
pub(super) fn measure<R>(ctx: &AmbientContext, f: impl FnOnce() -> R) -> R {
    // SAFETY: `_scope` is dropped before `ctx`.
    let _scope = unsafe { Scope::new(Target::Measured(ctx.as_ptr()), None) };
    f()
}

/// Run `f` on the context of the current scope, measured or not.
#[cfg_attr(debug_assertions, track_caller)]
pub(super) fn with_context<R>(what: &str, f: impl FnOnce(Option<&AmbientContext>) -> R) -> R {
    borrow(Target::current(what).node(), f)
}

/// Run `f` on the context of the current scope, if measured.
#[cfg_attr(debug_assertions, track_caller)]
pub(super) fn with_measured<R>(what: &str, f: impl FnOnce(Option<&AmbientContext>) -> R) -> R {
    borrow(Target::current(what).measured(), f)
}

fn borrow<R>(node: Option<NonNull<Node>>, f: impl FnOnce(Option<&AmbientContext>) -> R) -> R {
    // SAFETY: the pointer belongs to the innermost scope, which keeps it alive.
    f(node
        .map(|node| unsafe { AmbientContext::borrow_ptr(node) })
        .as_deref())
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
    /// The target, checked to be set: outside of any scope is a misuse.
    fn target(&self, what: &str, caller: &'static Location<'static>) -> Target {
        let target = self.target.get();
        cfg_select! {
            debug_assertions => match target {
                Target::Unset => super::missing_scope::panic_outside_of_any_scope(what, caller),
                Target::Unmeasured(_) | Target::Measured(_) => (),
            },
            _ => _ = (what, caller),
        }
        target
    }

    /// Drop the measurements and make every active scope stale.
    #[cold]
    fn poison(&self) {
        self.depth.set(self.depth.get() + (1 << 32));
        self.target.set(Target::Unmeasured(None));
        self.counters.iter().for_each(|c| c.set(0));
    }
}

impl Handoff {
    /// The current scope, to enter it elsewhere.
    #[cfg_attr(debug_assertions, track_caller)]
    pub(super) fn current() -> Self {
        Target::current("ambient::current()").to_handoff()
    }

    /// [`Self::current`] without the misuse check: outside of any scope is unmeasured.
    pub(super) fn current_unchecked() -> Self {
        Target::current_unchecked().to_handoff()
    }
}

/// A snapshot of the slot, valid until the next scope change on this thread.
#[derive(Clone, Copy)]
enum Target {
    /// The default state of a thread-local slot.
    /// Touching the slot in this state is a misuse:
    ///   it will panic in debug builds and unmeasured in release builds.
    Unset,
    /// Explicitly unmeasured.
    Unmeasured(Option<NonNull<Node>>),
    Measured(NonNull<Node>),
}

impl Target {
    /// The slot of this thread, checked to be set: outside of any scope is a misuse.
    #[cfg_attr(debug_assertions, track_caller)]
    fn current(what: &str) -> Self {
        let caller = Location::caller();
        SLOT.with(|slot| slot.target(what, caller))
    }

    fn current_unchecked() -> Self {
        SLOT.with(|slot| slot.target.get())
    }

    fn of(handoff: &Handoff) -> Self {
        match handoff {
            Handoff::Unmeasured(ctx) => {
                Target::Unmeasured(ctx.as_ref().map(AmbientContext::as_ptr))
            }
            Handoff::Measured(ctx) => Target::Measured(ctx.as_ptr()),
        }
    }

    fn to_handoff(self) -> Handoff {
        let clone = |node| {
            // SAFETY: the pointer belongs to the innermost scope, which keeps it alive.
            let ctx = unsafe { AmbientContext::borrow_ptr(node) };
            AmbientContext::clone(&*ctx)
        };
        match self {
            Target::Unset => Handoff::Unmeasured(None),
            Target::Unmeasured(node) => Handoff::Unmeasured(node.map(clone)),
            Target::Measured(node) => Handoff::Measured(clone(node)),
        }
    }

    fn node(self) -> Option<NonNull<Node>> {
        match self {
            Target::Unset => None,
            Target::Unmeasured(node) => node,
            Target::Measured(node) => Some(node),
        }
    }

    fn measured(self) -> Option<NonNull<Node>> {
        match self {
            Target::Unset | Target::Unmeasured(_) => None,
            Target::Measured(node) => Some(node),
        }
    }
}

/// An active scope. Restores the outer one on drop/panic.
#[must_use]
pub struct Scope {
    depth: u64,
    /// Restored on drop.
    outer_target: Target,
    /// Restored on drop.
    outer_counters: [usize; HwMetric::COUNT],
    /// Keeps the target alive for owned guard scopes.
    _owned: Option<Handoff>,
}

#[cfg(test)]
// Make sure that a future holding Scope across `.await` can't be spawned on a
// multi-threaded runtime.
static_assertions::assert_not_impl_any!(Scope: Send);

impl Scope {
    pub(super) fn enter_owned(handoff: Handoff) -> Self {
        // SAFETY: the handoff is moved into the scope.
        unsafe { Self::new(Target::of(&handoff), Some(handoff)) }
    }

    pub(super) fn unmeasured() -> Self {
        // SAFETY: the node belongs to the outer scope; dropping that first poisons the slot.
        unsafe { Self::new(Target::Unmeasured(Target::current_unchecked().node()), None) }
    }

    /// Mask the current scope, see [`super::parallel`].
    pub(super) fn masked() -> Self {
        // SAFETY: no node.
        unsafe { Self::new(Target::Unset, None) }
    }

    /// # Safety
    /// The node in `target` must outlive the scope, either:
    /// - via `owned`
    /// - via a borrow the caller holds for the scope's whole life
    /// - via an outer scope (dropping that first poisons the slot).
    unsafe fn new(target: Target, owned: Option<Handoff>) -> Self {
        SLOT.with(|slot| Self {
            depth: slot.depth.replace(slot.depth.get() + 1),
            outer_target: slot.target.replace(target),
            outer_counters: std::array::from_fn(|i| slot.counters[i].take()),
            _owned: owned,
        })
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        SLOT.with(|slot| {
            if slot.depth.get() != self.depth + 1 {
                slot.poison();
                debug_assert!(
                    std::thread::panicking(),
                    "ambient scopes exited out of order"
                );
                return;
            }
            slot.depth.set(self.depth);
            let counters: [usize; HwMetric::COUNT] =
                std::array::from_fn(|i| slot.counters[i].replace(self.outer_counters[i]));
            match slot.target.replace(self.outer_target) {
                Target::Measured(node) => {
                    if counters.iter().any(|&c| c != 0) {
                        // SAFETY: exited in order, so `node` belongs to this scope, which keeps it alive.
                        unsafe { AmbientContext::borrow_ptr(node) }
                            .accumulate(HardwareData(counters));
                    }
                }
                Target::Unset | Target::Unmeasured(_) => {}
            }
        });
    }
}
