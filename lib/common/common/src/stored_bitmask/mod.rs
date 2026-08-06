//! Compact persisted bitmask, written and read as a whole.
//!
//! The payload is a roaring bitmap of whichever bit value is the minority,
//! or raw dense bits when that is smaller — the file is never larger than
//! the dense representation.
//!
//! The file is never mutated in place: every save ([`save_bitmask`] or
//! [`MutableStoredBitmask::save`]) rewrites and replaces it atomically, so
//! an open always sees a consistent snapshot. [`MutableStoredBitmask`] is
//! the mutable in-RAM handle: it materializes the mask at open, collects
//! changes, and skips the rewrite when nothing changed.

mod format;
mod mutable;
mod read;
#[cfg(test)]
mod tests;
mod write;

pub use format::BitmaskContent;
pub use mutable::MutableStoredBitmask;
pub use read::StoredBitmask;
pub use write::save_bitmask;
