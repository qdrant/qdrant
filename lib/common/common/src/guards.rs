use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Nested guard holder.
pub struct NestedGuard<'main, Outer, Inner> {
    // Fields are dropped in the order of their definition.  It is important to drop the `inner_guard` first.
    inner_value: Inner,
    _outer_guard: Outer,
    phantom: PhantomData<&'main ()>,
}

impl<'main, Outer, Inner> NestedGuard<'main, Outer, Inner>
where
    Outer: 'main,
    Outer: Deref,
{
    /// Nested guard holder constructor.  Calling it is inherently unsafe.
    ///
    /// * Safety
    ///
    /// 1. It is safe if `outer_guard`'s `Deref` refers to some value that `outer_guard` refers to, but not to a `outer_guard`
    /// itself, i.e. `inner_value_fn` cannot refer to the `outer_guard`.
    /// 2. Also, it must be safe to drop the result of `inner_guard_fn` before the `outer_guard`.
    pub unsafe fn new(
        outer_guard: Outer,
        inner_value_fn: impl FnOnce(&'main <Outer as Deref>::Target) -> Inner,
    ) -> Self {
        let outer_ref = outer_guard.deref() as *const _;
        let inner_guard = unsafe { inner_value_fn(&*outer_ref) };

        Self {
            inner_value: inner_guard,
            _outer_guard: outer_guard,
            phantom: PhantomData,
        }
    }
}

impl<'main, Outer, Inner> Deref for NestedGuard<'main, Outer, Inner> {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner_value
    }
}

impl<'main, Outer, Inner> DerefMut for NestedGuard<'main, Outer, Inner> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner_value
    }
}
