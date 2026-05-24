//! Helper for in-place transform of `&mut T` via an owning closure.
//!
//! Used by the per-family `swap_on_disk` fast paths to flip an
//! `Immutable` enum variant to its `Mmap` sibling (or vice versa)
//! without rebuilding the index from payload storage.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::abort;
use std::ptr;

/// Replace `*slot` with `f(old).0`, returning `f(old).1`.
///
/// The closure receives the current value by ownership and must return a
/// `(new_value, return_value)` pair. `new_value` always replaces `*slot` —
/// even on the error branch the closure is responsible for returning a
/// well-defined value to put back.
///
/// If the closure panics the process aborts: `*slot` would otherwise be
/// left logically uninitialized (the old value has already been moved out
/// and there is no replacement to put back).
///
/// Same semantics as `take_mut::take_or_recover` — inlined here to avoid
/// pulling in the extra dependency.
#[inline]
pub(crate) fn try_replace<T, R, F: FnOnce(T) -> (T, R)>(slot: &mut T, f: F) -> R {
    // Safety: we move out of `*slot` with `ptr::read`, call `f` on a
    // wrapped panic-catching context, then `ptr::write` the new value
    // back. On panic we abort before `*slot` is observed again, so no
    // user code ever sees the half-initialized state.
    unsafe {
        let old = ptr::read(slot);
        match catch_unwind(AssertUnwindSafe(|| f(old))) {
            Ok((new, ret)) => {
                ptr::write(slot, new);
                ret
            }
            Err(_payload) => abort(),
        }
    }
}
