//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

#[cfg(test)]
mod fixtures;
