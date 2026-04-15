#![allow(dead_code)]
use std::error::Error;

use fst::Automaton;
use fst::automaton::Levenshtein;
use smallvec::SmallVec;

use super::prefix_chars;

pub const MAX_PATTERN_LENGTH: usize = 256;
pub const WILDCARD_ANY: char = '*';
pub const WILDCARD_ONE: char = '?';

pub struct PrefixLevenshtein {
    prefix: Vec<u8>,
    lev: Levenshtein,
}

impl PrefixLevenshtein {
    pub fn new(query: &str, prefix_len: usize, distance: u32) -> Result<Self, Box<dyn Error>> {
        let prefix = prefix_chars(query, prefix_len).as_bytes().to_vec();
        let lev = Levenshtein::new(query, distance)?;
        Ok(Self { prefix, lev })
    }
}

pub enum PrefixLevState {
    Prefix(usize),
    Fuzzy(<Levenshtein as Automaton>::State),
    Dead,
}

impl Automaton for PrefixLevenshtein {
    type State = PrefixLevState;

    fn start(&self) -> Self::State {
        if self.prefix.is_empty() {
            PrefixLevState::Fuzzy(self.lev.start())
        } else {
            PrefixLevState::Prefix(0)
        }
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match state {
            PrefixLevState::Prefix(_) => false,
            PrefixLevState::Fuzzy(s) => self.lev.is_match(s),
            PrefixLevState::Dead => false,
        }
    }

    fn can_match(&self, state: &Self::State) -> bool {
        match state {
            PrefixLevState::Prefix(_) => true,
            PrefixLevState::Fuzzy(s) => self.lev.can_match(s),
            PrefixLevState::Dead => false,
        }
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        match state {
            PrefixLevState::Prefix(_) => false,
            PrefixLevState::Fuzzy(s) => self.lev.will_always_match(s),
            PrefixLevState::Dead => false,
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        match state {
            PrefixLevState::Prefix(i) => {
                if byte == self.prefix[*i] {
                    let next = i + 1;
                    if next == self.prefix.len() {
                        let mut lev_state = self.lev.start();
                        for &b in &self.prefix {
                            lev_state = self.lev.accept(&lev_state, b);
                        }
                        PrefixLevState::Fuzzy(lev_state)
                    } else {
                        PrefixLevState::Prefix(next)
                    }
                } else {
                    PrefixLevState::Dead
                }
            }
            PrefixLevState::Fuzzy(s) => PrefixLevState::Fuzzy(self.lev.accept(s, byte)),
            PrefixLevState::Dead => PrefixLevState::Dead,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WildcardToken {
    /// A run of literal UTF-8 bytes that must match exactly.
    Literal(Vec<u8>),
    /// `?` – matches exactly one Unicode code point (1‒4 UTF-8 bytes).
    AnyOne,
    /// `*` – matches zero or more arbitrary bytes.
    AnyMany,
}

/// Parse a wildcard pattern string into a sequence of [`WildcardToken`]s.
///
/// * Consecutive `*` are collapsed into a single `AnyMany`.
/// * Adjacent literal characters are merged into one `Literal`.
fn parse_wildcard_pattern(pattern: &str) -> Vec<WildcardToken> {
    debug_assert!(
        !pattern.is_empty(),
        "wildcard patterns are validated by the API layer"
    );
    debug_assert!(
        pattern.len() <= MAX_PATTERN_LENGTH,
        "wildcard patterns are length-limited by the API layer"
    );

    let mut tokens: Vec<WildcardToken> = Vec::new();
    let mut literal_buf: Vec<u8> = Vec::new();

    for ch in pattern.chars() {
        match ch {
            WILDCARD_ANY => {
                if !literal_buf.is_empty() {
                    tokens.push(WildcardToken::Literal(std::mem::take(&mut literal_buf)));
                }
                // Collapse consecutive `*`
                if !matches!(tokens.last(), Some(WildcardToken::AnyMany)) {
                    tokens.push(WildcardToken::AnyMany);
                }
            }
            WILDCARD_ONE => {
                if !literal_buf.is_empty() {
                    tokens.push(WildcardToken::Literal(std::mem::take(&mut literal_buf)));
                }
                tokens.push(WildcardToken::AnyOne);
            }
            other => {
                let mut buf = [0u8; 4];
                literal_buf.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    if !literal_buf.is_empty() {
        tokens.push(WildcardToken::Literal(literal_buf));
    }
    tokens
}

/// Extract the longest literal prefix from a wildcard pattern (before the
/// first `*` or `?`).  Returns the prefix as a byte vector suitable for
/// `fst::StreamBuilder::ge()`.
pub fn extract_literal_prefix(pattern: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    for ch in pattern.chars() {
        if ch == WILDCARD_ANY || ch == WILDCARD_ONE {
            break;
        }
        let mut buf = [0u8; 4];
        prefix.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
    }
    prefix
}

/// A single "position" inside the NFA.
#[derive(Clone, Debug, PartialEq, Eq)]
enum AtomState {
    /// Positioned inside a token, with an optional byte offset into a
    /// `Literal` token.
    AtToken {
        token_index: usize,
        literal_offset: usize,
    },
    /// Mid-way through consuming a multi-byte UTF-8 code point for `AnyOne`.
    AtAnyOne { token_index: usize, remaining: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WildcardStateSet {
    atoms: SmallVec<[AtomState; 8]>,
}

pub struct WildcardAutomaton {
    tokens: Vec<WildcardToken>,
    literal_prefix: Vec<u8>,
}

impl WildcardAutomaton {
    /// Build a new automaton from a wildcard pattern.
    pub fn new(pattern: &str) -> Self {
        let tokens = parse_wildcard_pattern(pattern);
        let literal_prefix = extract_literal_prefix(pattern);
        Self {
            tokens,
            literal_prefix,
        }
    }

    /// The literal prefix extracted from the pattern, useful for `.ge()` on
    /// FST streams.
    pub fn literal_prefix(&self) -> &[u8] {
        &self.literal_prefix
    }

    /// Number of tokens in this automaton (exposed for tests).
    #[cfg(test)]
    fn token_count(&self) -> usize {
        self.tokens.len()
    }

    /// Compute the epsilon-closure of the current NFA frontier.
    ///
    /// In this wildcard automaton, the only epsilon transition comes from `*`
    /// (`WildcardToken::AnyMany`): besides the branch that keeps consuming more
    /// input while staying on `*`, there is also a zero-cost branch that skips
    /// over `*` and moves directly to the next token.
    ///
    /// This function repeatedly expands that zero-cost branch for every atom in
    /// `atoms` until no new reachable state can be added.
    ///
    /// Example:
    ///
    /// Suppose the pattern is `ab*cd`, which is tokenized as:
    ///
    /// - `0 => Literal("ab")`
    /// - `1 => AnyMany`
    /// - `2 => Literal("cd")`
    ///
    /// If the current frontier contains:
    ///
    /// - `AtToken { token_index: 1, literal_offset: 0 }`
    ///
    /// it means the automaton is currently positioned on `*`.
    /// At this point there are two legal possibilities:
    ///
    /// - stay on token `1` and let `*` consume one more input byte later;
    /// - skip token `1` immediately and continue from token `2`.
    ///
    /// `epsilon_closure` adds that second possibility by inserting:
    ///
    /// - `AtToken { token_index: 2, literal_offset: 0 }`
    ///
    /// into the frontier.
    ///
    /// The `while i < atoms.len()` loop is important: newly inserted states are
    /// scanned as well, so chained stars such as `a**b` (collapsed during
    /// parsing) or any future epsilon-producing tokens would also be fully
    /// expanded before the function returns.
    ///
    /// Only atoms exactly at the start of a token (`literal_offset == 0`) are
    /// considered. A state inside a literal byte sequence cannot take an
    /// epsilon transition because it must finish matching the remaining bytes of
    /// that literal first.
    fn epsilon_closure(&self, mut atoms: SmallVec<[AtomState; 8]>) -> SmallVec<[AtomState; 8]> {
        let mut i = 0;
        while i < atoms.len() {
            if let AtomState::AtToken {
                token_index,
                literal_offset: 0,
            } = atoms[i]
            {
                if token_index < self.tokens.len() {
                    if let WildcardToken::AnyMany = &self.tokens[token_index] {
                        let next = AtomState::AtToken {
                            token_index: token_index + 1,
                            literal_offset: 0,
                        };
                        if !atoms.contains(&next) {
                            atoms.push(next);
                        }
                    }
                }
            }
            i += 1;
        }
        atoms
    }
}

/// Determine the expected total byte length of a UTF-8 code point from its
/// leading byte.  Returns `None` for invalid leading bytes.
#[inline]
fn utf8_char_len(first_byte: u8) -> Option<u8> {
    match first_byte {
        0x00..=0x7F => Some(1),
        0xC2..=0xDF => Some(2),
        0xE0..=0xEF => Some(3),
        0xF0..=0xF4 => Some(4),
        _ => None,
    }
}

#[inline]
fn is_utf8_continuation(byte: u8) -> bool {
    byte & 0xC0 == 0x80
}

impl Automaton for WildcardAutomaton {
    type State = WildcardStateSet;

    fn start(&self) -> Self::State {
        let init = SmallVec::from_elem(
            AtomState::AtToken {
                token_index: 0,
                literal_offset: 0,
            },
            1,
        );
        WildcardStateSet {
            atoms: self.epsilon_closure(init),
        }
    }

    fn is_match(&self, state: &Self::State) -> bool {
        let end_index = self.tokens.len();
        state.atoms.iter().any(|a| {
            matches!(a, AtomState::AtToken { token_index, literal_offset: 0 } if *token_index == end_index)
        })
    }

    fn can_match(&self, state: &Self::State) -> bool {
        !state.atoms.is_empty()
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        // A trailing `*` that has already consumed all subsequent tokens will
        // always match regardless of remaining input.
        let end_index = self.tokens.len();
        state.atoms.iter().any(|a| {
            // We are *at* an AnyMany token and the ε-closure already pushed
            // the end sentinel, so the AnyMany is the very last real token.
            if let AtomState::AtToken {
                token_index,
                literal_offset: 0,
            } = a
            {
                if *token_index < end_index {
                    // Current token is AnyMany AND the next position (end) is
                    // also in the set, meaning ε-closure reached the end.
                    if let WildcardToken::AnyMany = &self.tokens[*token_index] {
                        return state.atoms.contains(&AtomState::AtToken {
                            token_index: end_index,
                            literal_offset: 0,
                        });
                    }
                }
            }
            false
        })
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        let mut next: SmallVec<[AtomState; 8]> = SmallVec::new();

        for atom in &state.atoms {
            match atom {
                AtomState::AtToken {
                    token_index,
                    literal_offset,
                } => {
                    let tidx = *token_index;
                    let loff = *literal_offset;
                    if tidx >= self.tokens.len() {
                        // Past the end – no transition.
                        continue;
                    }
                    match &self.tokens[tidx] {
                        WildcardToken::Literal(bytes) => {
                            if loff < bytes.len() && bytes[loff] == byte {
                                let new_off = loff + 1;
                                let new_atom = if new_off == bytes.len() {
                                    AtomState::AtToken {
                                        token_index: tidx + 1,
                                        literal_offset: 0,
                                    }
                                } else {
                                    AtomState::AtToken {
                                        token_index: tidx,
                                        literal_offset: new_off,
                                    }
                                };
                                if !next.contains(&new_atom) {
                                    next.push(new_atom);
                                }
                            }
                        }
                        WildcardToken::AnyOne => {
                            // Start consuming a UTF-8 code point.
                            if let Some(total) = utf8_char_len(byte) {
                                if total == 1 {
                                    // ASCII – advance to next token immediately.
                                    let new_atom = AtomState::AtToken {
                                        token_index: tidx + 1,
                                        literal_offset: 0,
                                    };
                                    if !next.contains(&new_atom) {
                                        next.push(new_atom);
                                    }
                                } else {
                                    let new_atom = AtomState::AtAnyOne {
                                        token_index: tidx,
                                        remaining: total - 1,
                                    };
                                    if !next.contains(&new_atom) {
                                        next.push(new_atom);
                                    }
                                }
                            }
                            // Invalid leading byte → drop this path.
                        }
                        WildcardToken::AnyMany => {
                            // Consume the byte and stay at the same `*`.
                            let stay = AtomState::AtToken {
                                token_index: tidx,
                                literal_offset: 0,
                            };
                            if !next.contains(&stay) {
                                next.push(stay);
                            }
                        }
                    }
                }
                AtomState::AtAnyOne {
                    token_index,
                    remaining,
                } => {
                    if is_utf8_continuation(byte) {
                        if *remaining == 1 {
                            let new_atom = AtomState::AtToken {
                                token_index: token_index + 1,
                                literal_offset: 0,
                            };
                            if !next.contains(&new_atom) {
                                next.push(new_atom);
                            }
                        } else {
                            let new_atom = AtomState::AtAnyOne {
                                token_index: *token_index,
                                remaining: remaining - 1,
                            };
                            if !next.contains(&new_atom) {
                                next.push(new_atom);
                            }
                        }
                    }
                    // Non-continuation byte → invalid UTF-8, drop path.
                }
            }
        }

        WildcardStateSet {
            atoms: self.epsilon_closure(next),
        }
    }
}

/// Simple helper that runs the automaton byte-by-byte over a string.
/// Useful for the mutable (BTreeSet) backend and for unit tests.
pub fn wildcard_matches(pattern: &str, input: &str) -> bool {
    let automaton = WildcardAutomaton::new(pattern);
    let mut state = automaton.start();
    for &b in input.as_bytes() {
        state = automaton.accept(&state, b);
        if !automaton.can_match(&state) {
            return false;
        }
    }
    automaton.is_match(&state)
}

#[cfg(test)]
mod tests {
    use fst::{IntoStreamer, Set, Streamer};

    use super::*;

    fn fst_search(terms: &[&str], pattern: &str) -> Vec<String> {
        let mut sorted: Vec<&str> = terms.to_vec();
        sorted.sort();
        sorted.dedup();
        let set = Set::from_iter(sorted).unwrap();
        let automaton = WildcardAutomaton::new(pattern);
        let prefix = automaton.literal_prefix().to_vec();
        if prefix.is_empty() {
            let mut stream = set.search(&automaton).into_stream();
            let mut results = Vec::new();
            while let Some(term_bytes) = stream.next() {
                results.push(String::from_utf8(term_bytes.to_vec()).unwrap());
            }
            results
        } else {
            let mut stream = set.search(&automaton).ge(&prefix).into_stream();
            let mut results = Vec::new();
            while let Some(term_bytes) = stream.next() {
                results.push(String::from_utf8(term_bytes.to_vec()).unwrap());
            }
            results
        }
    }

    // Helper: check wildcard_matches for a list of (input, expected) pairs.
    fn assert_matches(pattern: &str, cases: &[(&str, bool)]) {
        for &(input, expected) in cases {
            let result = wildcard_matches(pattern, input);
            assert_eq!(
                result, expected,
                "pattern={pattern:?} input={input:?}: expected {expected}, got {result}"
            );
        }
    }

    #[test]
    fn test_consecutive_stars_collapsed() {
        let a = WildcardAutomaton::new("a**b***c");
        // Tokens should be: Literal("a"), AnyMany, Literal("b"), AnyMany, Literal("c")
        assert_eq!(a.token_count(), 5);
    }

    #[test]
    fn test_prefix_extraction() {
        assert_eq!(extract_literal_prefix("foo*"), b"foo");
        assert_eq!(extract_literal_prefix("foo?bar"), b"foo");
        assert_eq!(extract_literal_prefix("*foo"), b"");
        assert_eq!(extract_literal_prefix("?foo"), b"");
        assert_eq!(extract_literal_prefix("abc"), b"abc");
        assert_eq!(extract_literal_prefix("你好*"), "你好".as_bytes());
    }

    #[test]
    fn test_exact_and_star_matches() {
        assert_matches(
            "hello",
            &[("hello", true), ("hell", false), ("helloo", false)],
        );
        assert_matches(
            "foo*",
            &[
                ("foo", true),
                ("foobar", true),
                ("food", true),
                ("fo", false),
                ("bar", false),
            ],
        );
        assert_matches(
            "*foo",
            &[
                ("foo", true),
                ("xfoo", true),
                ("barfoo", true),
                ("foobar", false),
                ("fo", false),
            ],
        );
        assert_matches(
            "f*o",
            &[
                ("fo", true),
                ("foo", true),
                ("fao", true),
                ("fabco", true),
                ("f", false),
                ("o", false),
                ("fox", false),
            ],
        );
    }

    #[test]
    fn test_question_matches_single_unicode_scalar() {
        assert_matches(
            "你?好",
            &[
                ("你们好", true),
                ("你x好", true),
                ("你好", false),
                ("你ab好", false),
            ],
        );
    }

    #[test]
    fn test_complex_pattern() {
        assert_matches(
            "a*b?c*d",
            &[
                ("abxcd", true),
                ("aXXXbYcZZZd", true),
                ("abcd", false),
                ("abxc", false),
            ],
        );
    }

    #[test]
    fn test_fst_star_prefix_recall() {
        let vocab = ["apple", "afoo", "foo", "xfoo"];
        let results = fst_search(&vocab, "*foo");
        assert!(results.contains(&"afoo".to_string()));
        assert!(results.contains(&"foo".to_string()));
        assert!(results.contains(&"xfoo".to_string()));
        assert!(!results.contains(&"apple".to_string()));
    }

    #[test]
    fn test_fst_unicode_question() {
        let vocab = ["你们好", "你x好", "你好", "你ab好"];
        let results = fst_search(&vocab, "你?好");
        assert!(results.contains(&"你们好".to_string()));
        assert!(results.contains(&"你x好".to_string()));
        assert!(!results.contains(&"你好".to_string()));
        assert!(!results.contains(&"你ab好".to_string()));
    }

    #[test]
    fn test_will_always_match_trailing_star() {
        let a = WildcardAutomaton::new("foo*");
        let mut state = a.start();
        for &b in b"foo" {
            state = a.accept(&state, b);
        }
        assert!(a.will_always_match(&state));
    }

    #[test]
    fn test_emoji_question() {
        assert_matches("?", &[("😀", true)]);
        assert_matches("a?b", &[("a😀b", true), ("ab", false)]);
    }

    #[test]
    fn test_worst_case_no_match_star_pattern() {
        let input = "a".repeat(50);
        let result = wildcard_matches("*a*a*a*a*b", &input);
        assert!(!result);
    }

    #[test]
    fn test_dead_state_on_mismatch() {
        let a = WildcardAutomaton::new("abc");
        let mut state = a.start();
        state = a.accept(&state, b'x');
        assert!(!a.can_match(&state));
    }
}
