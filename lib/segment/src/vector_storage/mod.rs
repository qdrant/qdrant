pub mod chunked_vectors;
pub mod memmap_vector_storage;
mod mmap_vectors;
pub mod quantized;
pub mod raw_scorer;
pub mod simple_vector_storage;
mod vector_storage_base;

pub use raw_scorer::*;
pub use vector_storage_base::*;

/// Calculates the quotient of self and rhs, rounding the result towards positive infinity.
///
/// # Panics
///
/// This function will panic if rhs is zero.
///
/// # Overflow behavior
///
/// On overflow, this function will panic if overflow checks are enabled (default in debug mode)
/// and wrap if overflow checks are disabled (default in release mode).
// Can be replaced with stdlib once it stabilizes:
// - <https://github.com/rust-lang/rust/issues/88581>
// - <https://doc.rust-lang.org/std/primitive.usize.html#method.div_ceil>
#[inline]
const fn div_ceil(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}
