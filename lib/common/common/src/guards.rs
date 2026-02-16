use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Owning guard holder.
pub struct OwningGuard<'ctx, Outer, Inner>
where
    Outer: 'ctx,
{
    // Fields are dropped in the order of their definition.  It is important to drop the `inner_value` first.
    inner_value: Inner,
    _outer_guard: Outer,
    phantom: PhantomData<&'ctx ()>,
}

impl<'ctx, Outer, Inner> OwningGuard<'ctx, Outer, Inner>
where
    Outer: 'ctx,
    Outer: Deref,
{
    /// Owning guard holder constructor.
    ///
    /// # Safety
    ///
    /// 1. It is safe if `outer_guard`'s `Deref` refers to some value outside of `outer_guard`, but not to
    ///    inside `outer_guard` itself.
    /// 2. Also, it must be safe to drop the result of `inner_guard_fn` before the `outer_guard`.
    /// 3. The argument of the `inner_guard_fn` must not escape from the `inner_guard_fn` itself except
    ///    for the return value.
    pub unsafe fn new(
        outer_guard: Outer,
        inner_value_fn: impl FnOnce(&'ctx <Outer as Deref>::Target) -> Inner,
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

impl<'ctx, Outer, Inner> Deref for OwningGuard<'ctx, Outer, Inner> {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner_value
    }
}

impl<'ctx, Outer, Inner> DerefMut for OwningGuard<'ctx, Outer, Inner> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner_value
    }
}
