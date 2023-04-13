pub mod chunked_vectors;
pub mod memmap_vector_storage;
mod mmap_vectors;
pub mod quantized;
pub mod raw_scorer;
pub mod simple_vector_storage;
mod vector_storage_base;

pub use raw_scorer::*;
pub use vector_storage_base::*;

// We can replace this by `div_ceil` from the standard library once it stabilizes.
#[inline]
const fn div_ceil(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}
