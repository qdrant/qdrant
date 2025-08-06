//! Different flavors of flags structures, akin to a Vec<bool>, but persistent and efficient.
//!
//! Here's a brief overview of the different flavors of flags structures:
//! - `dynamic_mmap_flags`: Base implementation of storage in mmapped files.
//! - `buffered_dynamic_flags`: Builds on top of `dynamic_mmap_flags` to provide buffered writes.
//! - `bitvec_flags`: `buffered_dynamic_flags` with in-memory bitvec for reads.
//! - `roaring_flags`: `buffered_dynamic_flags` with in-memory roaring bitmap for reads.

pub mod bitvec_flags;
mod buffered_dynamic_flags;
pub mod dynamic_mmap_flags;
pub mod roaring_flags;
