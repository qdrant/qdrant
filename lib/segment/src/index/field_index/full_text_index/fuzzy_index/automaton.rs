use std::error::Error;

use fst::Automaton;
use fst::automaton::Levenshtein;

use super::prefix_chars;

/// Levenshtein automaton that requires an exact byte prefix before fuzzy
/// matching starts.
pub struct PrefixLevenshtein {
    prefix: Vec<u8>,
    lev: Levenshtein,
}

impl PrefixLevenshtein {
    /// Builds an automaton that enforces `prefix_len` exact UTF-8 characters and
    /// then allows up to `distance` edits for the full query.
    pub fn new(query: &str, prefix_len: usize, distance: u32) -> Result<Self, Box<dyn Error>> {
        let prefix = prefix_chars(query, prefix_len).as_bytes().to_vec();
        let lev = Levenshtein::new(query, distance)?;
        Ok(Self { prefix, lev })
    }
}

/// State machine for [`PrefixLevenshtein`].
pub enum PrefixLevState {
    /// Still consuming the exact prefix bytes.
    Prefix(usize),
    /// Exact prefix is complete; delegate to the inner Levenshtein automaton.
    Fuzzy(<Levenshtein as Automaton>::State),
    /// Prefix mismatch made the candidate unreachable.
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

    /// Returns true if and only if state is a match state.
    ///
    /// Examples with query `"cart"`, `prefix_len = 2`, `distance = 1`:
    /// - after reading `"c"`: state is `Prefix(1)`, so `is_match = false`
    /// - after reading `"ca"`: state moves to `Fuzzy(...)`, but `is_match` still
    ///   depends on the inner Levenshtein state
    /// - after reading `"card"`: state is `Fuzzy(...)` and `is_match = true`
    fn is_match(&self, state: &Self::State) -> bool {
        match state {
            PrefixLevState::Prefix(_) => false,
            PrefixLevState::Fuzzy(s) => self.lev.is_match(s),
            PrefixLevState::Dead => false,
        }
    }

    /// Returns true if and only if `state` can still reach a match in zero or
    /// more future steps.
    ///
    /// Examples with query `"cart"`, `prefix_len = 2`, `distance = 1`:
    /// - after reading `"c"`: state is `Prefix(1)`, so `can_match = true`
    ///   because another `"a"` can continue the exact prefix path
    /// - after reading `"d"`: state is `Dead`, so `can_match = false`
    /// - after reading `"ca"`: state is `Fuzzy(...)`; whether it can still match
    ///   is delegated to the inner Levenshtein automaton
    ///
    /// If this returns false, then no sequence of future inputs should ever
    /// produce a match. Returning true when no match is actually possible is
    /// still correct, but it may force callers to do extra work.
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

    /// Consumes one byte and returns the next automaton state.
    ///
    /// Examples with query `"cart"`, `prefix_len = 2`, `distance = 1`:
    /// - `Prefix(0)` + `'c'` -> `Prefix(1)`
    /// - `Prefix(1)` + `'a'` -> `Fuzzy(...)` because the exact prefix `"ca"`
    ///   is now complete
    /// - `Prefix(0)` + `'d'` -> `Dead` because the exact prefix mismatched
    /// - `Fuzzy(s)` + `'r'` -> advances only the inner Levenshtein state
    /// - `Dead` + any byte -> `Dead`
    ///
    /// The transition to `Fuzzy(...)` replays the exact prefix into the inner
    /// Levenshtein automaton so that fuzzy matching continues from the full
    /// query prefix that has already been consumed.
    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        match state {
            PrefixLevState::Prefix(i) => {
                // While we are in the exact-prefix phase, every byte must match
                // the corresponding byte from `self.prefix`.
                if byte == self.prefix[*i] {
                    let next = i + 1;
                    if next == self.prefix.len() {
                        // The exact prefix is fully consumed. Switch to fuzzy
                        // matching, but first replay the accepted prefix bytes
                        // into the inner Levenshtein automaton so its state is
                        // aligned with the input consumed so far.
                        let mut lev_state = self.lev.start();
                        for &b in &self.prefix {
                            lev_state = self.lev.accept(&lev_state, b);
                        }
                        PrefixLevState::Fuzzy(lev_state)
                    } else {
                        // Still consuming the exact prefix.
                        PrefixLevState::Prefix(next)
                    }
                } else {
                    // Any mismatch in the required prefix makes this path
                    // unreachable.
                    PrefixLevState::Dead
                }
            }
            // Once the exact prefix is done, delegate all future bytes to the
            // inner Levenshtein automaton.
            PrefixLevState::Fuzzy(s) => PrefixLevState::Fuzzy(self.lev.accept(s, byte)),
            // Dead is a sink state.
            PrefixLevState::Dead => PrefixLevState::Dead,
        }
    }
}

#[cfg(test)]
mod tests {
    use fst::Automaton;

    use super::{PrefixLevState, PrefixLevenshtein};

    fn accept_str(automaton: &PrefixLevenshtein, input: &str) -> PrefixLevState {
        let mut state = automaton.start();
        for byte in input.bytes() {
            state = automaton.accept(&state, byte);
        }
        state
    }

    #[test]
    fn prefix_state_can_match_but_is_not_match() {
        let automaton = PrefixLevenshtein::new("abc", 2, 1).unwrap();

        let state = accept_str(&automaton, "a");

        assert!(!automaton.is_match(&state));
        assert!(automaton.can_match(&state));
    }

    #[test]
    fn full_prefix_transitions_to_fuzzy_with_true_is_match_and_can_match() {
        let automaton = PrefixLevenshtein::new("ab", 2, 1).unwrap();

        let state = accept_str(&automaton, "ab");

        assert!(matches!(state, PrefixLevState::Fuzzy(_)));
        assert!(automaton.is_match(&state));
        assert!(automaton.can_match(&state));
    }

    #[test]
    fn prefix_mismatch_goes_dead() {
        let automaton = PrefixLevenshtein::new("abc", 2, 1).unwrap();

        let state = accept_str(&automaton, "x");

        assert!(matches!(state, PrefixLevState::Dead));
        assert!(!automaton.is_match(&state));
        assert!(!automaton.can_match(&state));
    }

    #[test]
    fn fuzzy_matching_starts_after_exact_prefix() {
        let automaton = PrefixLevenshtein::new("abc", 2, 1).unwrap();

        let state = accept_str(&automaton, "abd");

        assert!(matches!(state, PrefixLevState::Fuzzy(_)));
        assert!(automaton.is_match(&state));
    }

    #[test]
    fn unicode_prefix_length_uses_chars_not_bytes() {
        let automaton = PrefixLevenshtein::new("éclair", 1, 1).unwrap();

        let state = accept_str(&automaton, "éclair");

        assert!(matches!(state, PrefixLevState::Fuzzy(_)));
        assert!(automaton.is_match(&state));
    }
}
