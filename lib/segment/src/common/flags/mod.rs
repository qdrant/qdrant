//! Different flavors of flags structures, akin to a Vec<bool>, but persistent and efficient.
//!
//! Here's a brief overview of the different flavors of flags structures:
//! - `dynamic_mmap_flags`: Base implementation of storage in mmapped files.
//! - `buffered_dynamic_flags`: Builds on top of `dynamic_mmap_flags` to provide buffered writes.
//! - `bitvec_flags`: `buffered_dynamic_flags` with in-memory bitvec for reads.
//! - `roaring_flags`: `buffered_dynamic_flags` with in-memory roaring bitmap for reads.
//! - `in_memory_bitvec_flags`: in-memory counterpart of `bitvec_flags`, bound to `UniversalRead`.
//! - `read_only_roaring_flags`: read-only counterpart of `roaring_flags`, bound to `UniversalRead`.
//! - `compact_stored_flags`: RAM-resident flags over a single compact stored-bitmask file,
//!   rewritten whole on flush; serverless-compatible counterpart of the dynamic + buffered stack.
//! - `read_only_compact_flags`: read-only counterpart of `compact_stored_flags`, bound to
//!   `UniversalRead`.
//! - `read_only_flags`: mode-dispatching union of `read_only_roaring_flags` and
//!   `read_only_compact_flags`.
//!
//! `bitvec_flags` and `roaring_flags` persist either through the dynamic stack or through
//! `compact_stored_flags`, selected by [`FlagsMode`] when flags are created and detected
//! automatically when opening existing flags. The read-only types (`read_only_flags`,
//! `in_memory_bitvec_flags`) likewise detect the mode of the flags they open.

pub mod bitvec_flags;
mod buffered_dynamic_flags;
pub mod compact_stored_flags;
pub mod dynamic_stored_flags;
pub mod in_memory_bitvec_flags;
mod mode;
pub mod read_only_compact_flags;
pub mod read_only_flags;
pub mod read_only_roaring_flags;
pub mod roaring_flags;
mod storage;
pub mod update_only_stored_flags;

pub use mode::FlagsMode;
