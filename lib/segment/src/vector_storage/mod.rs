pub mod appendable_mmap_vector_storage;
#[cfg(target_os = "linux")]
pub mod async_raw_scorer;
mod chunked_mmap_vectors;
mod chunked_utils;
pub mod chunked_vectors;
mod dynamic_mmap_flags;
pub mod memmap_vector_storage;
mod mmap_vectors;
pub mod quantized;
pub mod raw_scorer;
pub mod simple_vector_storage;
mod vector_storage_base;

#[cfg(test)]
mod tests;

#[cfg(target_os = "linux")]
mod async_io;
mod async_io_mock;
mod bitvec;
pub mod common;
pub mod query;
mod query_scorer;
pub mod simple_sparse_vector_storage;
pub mod sparse_raw_scorer;

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
pub(crate) const fn div_ceil(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}
