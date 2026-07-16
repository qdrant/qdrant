//! Compact persisted bitmask, written and read as a whole.
//!
//! The payload is a roaring bitmap of whichever bit value is the minority,
//! or raw dense bits when that is smaller — the file is never larger than
//! the dense representation.
//!
//! There is no in-place mutation: [`save_bitmask`] replaces the file
//! atomically, so an open always sees a consistent snapshot.

mod format;
mod read;
#[cfg(test)]
mod tests;
mod write;

pub use format::BitmaskContent;
pub use read::StoredBitmask;
pub use write::save_bitmask;
