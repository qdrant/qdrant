//! Regular-expression matching over the keys of the keyword index.
//!
//! A regex condition is answered from the index's distinct keys, never from
//! the payload: select the keys the regex matches, then union their postings.
//! To avoid checking every key, a trigram index over the keys narrows them
//! down to candidates first — including for unanchored patterns like
//! `.*foo.*`, which a sorted dictionary can't bound.
//!
//! # Matching a regex
//!
//! 1. The pattern is compiled once per query into a [`RegexMatcher`], which
//!    also derives a [`TrigramQuery`]: a boolean query over trigrams that every
//!    matching key satisfies (see [`query`](self::query)).
//! 2. The query selects candidate keys from the trigram index.
//! 3. Each candidate is verified with the regex against the key itself, which
//!    the index stores, so no payload is read.
//!
//! Patterns without a required 3-byte substring (`^.{3}$`, `\d+`) have the
//! query [`TrigramQuery::All`]: every key is a candidate.
//!
//! # Module layout
//!
//! - [`trigram`](self::trigram) — trigrams of a key: folded, wrapped in
//!   boundary markers;
//! - [`query`](self::query) — regex → trigram query analysis;
//! - [`matcher`](self::matcher) — [`RegexMatcher`], the compiled pattern and
//!   its safety limits.

mod matcher;
mod query;
mod trigram;

#[cfg(test)]
mod tests;

pub use self::matcher::{MAX_REGEX_LEN, RegexError, RegexMatcher};
pub use self::query::{MAX_QUERY_TRIGRAMS, TrigramQuery};
pub use self::trigram::{BOUNDARY, Trigram, fold, key_trigrams};
