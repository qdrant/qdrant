use std::mem::MaybeUninit;

/// [`MaybeUninit::fill_from`] backported to stable.
///
/// TODO: remove in favor of [`MaybeUninit::fill_from`] once stabilized.
/// <https://github.com/rust-lang/rust/issues/117428>
pub fn maybe_uninit_fill_from<I: IntoIterator>(
    this: &mut [MaybeUninit<I::Item>],
    it: I,
) -> (&mut [I::Item], &mut [MaybeUninit<I::Item>]) {
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
    (unsafe { assume_init_mut(initted) }, remainder)
}

/// [`<[MaybeUninit]>::assume_init_mut`] backported to stable.
/// <https://github.com/rust-lang/rust/issues/63569>
///
/// Gets a mutable (unique) reference to the contained value.
///
/// # Safety
///
/// Calling this when the content is not yet fully initialized causes undefined
/// behavior: it is up to the caller to guarantee that every `MaybeUninit<T>` in the
/// slice really is in an initialized state. For instance, `.assume_init_mut()` cannot
/// be used to initialize a `MaybeUninit` slice.
#[inline(always)]
pub const unsafe fn assume_init_mut<T>(this: &mut [MaybeUninit<T>]) -> &mut [T] {
    // SAFETY: similar to safety notes for `slice_get_ref`, but we have a
    // mutable reference which is also guaranteed to be valid for writes.
    unsafe { &mut *(this as *mut [MaybeUninit<T>] as *mut [T]) }
}
