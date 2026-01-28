use std::mem::MaybeUninit;

/// Backported ustable method [`std::slice::write_iter`] (was [`MaybeUninit::fill_from`]).
/// <https://github.com/rust-lang/rust/blob/1.93.0/library/core/src/mem/maybe_uninit.rs#L1368>
///
/// TODO: remove in favor of [`std::slice::write_iter`] once stabilized.
/// <https://github.com/rust-lang/rust/issues/117428>
pub fn maybe_uninit_fill_from<I: IntoIterator>(
    this: &mut [MaybeUninit<I::Item>],
    it: I,
) -> (&mut [I::Item], &mut [MaybeUninit<I::Item>]) {
    let iter = it.into_iter();
    let mut guard = Guard {
        slice: this,
        initialized: 0,
    };

    for (element, val) in guard.slice.iter_mut().zip(iter) {
        element.write(val);
        guard.initialized += 1;
    }

    let initialized_len = guard.initialized;
    std::mem::forget(guard);

    // SAFETY: guard.initialized <= this.len()
    let (initted, remainder) = unsafe { this.split_at_mut_unchecked(initialized_len) };

    // SAFETY: Valid elements have just been written into `init`, so that portion
    // of `this` is initialized.
    (unsafe { assume_init_mut(initted) }, remainder)
}

/// Backported private struct.
/// <https://github.com/rust-lang/rust/blob/1.93.0/library/core/src/mem/maybe_uninit.rs#L1556>
struct Guard<'a, T> {
    slice: &'a mut [MaybeUninit<T>],
    initialized: usize,
}

impl<'a, T> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        let initialized_part = &mut self.slice[..self.initialized];
        // SAFETY: this raw sub-slice will contain only initialized objects.
        unsafe { assume_init_drop(initialized_part) }
    }
}

/// Backported unstable method [`std::slice::assume_init_drop`].
/// <https://github.com/rust-lang/rust/blob/1.93.0/library/core/src/mem/maybe_uninit.rs#L808>
#[inline(always)]
unsafe fn assume_init_drop<T>(this: &mut [MaybeUninit<T>]) {
    if !this.is_empty() {
        // SAFETY: the caller must guarantee that every element of `this`
        // is initialized and satisfies all invariants of `T`.
        // Dropping the value in place is safe if that is the case.
        unsafe { std::ptr::drop_in_place(this as *mut [MaybeUninit<T>] as *mut [T]) }
    }
}

/// Backported unstable method [`std::slice::assume_init_mut`].
/// <https://github.com/rust-lang/rust/blob/1.93.0/library/core/src/mem/maybe_uninit.rs#L993>
#[inline(always)]
unsafe fn assume_init_mut<T>(this: &mut [MaybeUninit<T>]) -> &mut [T] {
    // SAFETY: similar to safety notes for `slice_get_ref`, but we have a
    // mutable reference which is also guaranteed to be valid for writes.
    unsafe { &mut *(this as *mut [MaybeUninit<T>] as *mut [T]) }
}
