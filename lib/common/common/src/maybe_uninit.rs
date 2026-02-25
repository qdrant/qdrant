use std::mem::{MaybeUninit, transmute};
use std::ops::{Deref, DerefMut};

/// [`MaybeUninit::fill_from`] backported to stable.
///
/// Unlike the standard library version, this function does not support [`Drop`]
/// types, for simplicity of implementation.
///
/// TODO: remove in favor of [`MaybeUninit::fill_from`] once stabilized.
/// <https://github.com/rust-lang/rust/issues/117428>
pub fn maybe_uninit_fill_from<I: IntoIterator>(
    this: &mut [MaybeUninit<I::Item>],
    it: I,
) -> (&mut [I::Item], &mut [MaybeUninit<I::Item>]) {
    const { assert!(!std::mem::needs_drop::<I::Item>(), "Not supported") };

    let iter = it.into_iter();

    let mut initialized_len = 0;
    for (element, val) in this.iter_mut().zip(iter) {
        element.write(val);
        initialized_len += 1;
    }

    // SAFETY: guard.initialized <= this.len()
    let (initted, remainder) = unsafe { this.split_at_mut_unchecked(initialized_len) };

    // SAFETY: Valid elements have just been written into `init`, so that portion
    // of `this` is initialized.
    (
        unsafe { transmute::<&mut [MaybeUninit<I::Item>], &mut [I::Item]>(initted) },
        remainder,
    )
}

pub type InitUninit<'a, T> = (RefDropper<'a, [T]>, &'a mut [MaybeUninit<T>]);

/// Wrapper around [`maybe_uninit_fill_from`] that doesn't leak on drop.
#[inline(always)]
pub fn maybe_uninit_fill_from_with_drop<I: IntoIterator>(
    this: &mut [MaybeUninit<I::Item>],
    it: I,
) -> InitUninit<'_, I::Item> {
    let (initted, remainder) = maybe_uninit_fill_from(this, it);
    (RefDropper(initted), remainder)
}

pub struct RefDropper<'a, T: ?Sized>(&'a mut T);

impl<'a, T: ?Sized> Deref for RefDropper<'a, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: ?Sized> DerefMut for RefDropper<'a, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'a, T: ?Sized> Drop for RefDropper<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { std::ptr::drop_in_place(self.0) }
    }
}
