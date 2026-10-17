//! [`RegexMatcher`]: a user pattern compiled once per query, with the limits
//! that make untrusted patterns safe to run.

use regex::{Regex, RegexBuilder};

use super::query::TrigramQuery;
use super::re2::to_engine_pattern;

/// Longest accepted pattern, in bytes.
pub const MAX_REGEX_LEN: usize = 1024;

/// Limit of the compiled program. Matching time is linear in the program
/// size, so this bounds the cost per checked value; Unicode classes under
/// counted repetition (`\pL{1000}`) are what hits it.
const COMPILED_SIZE_LIMIT: usize = 10 << 20;

/// Cache of the lazy DFA. Too small a cache makes the engine fall back to a
/// much slower one on Unicode-heavy patterns: a Unicode `\w{1,100}x` over 1M
/// keys took 3.9 s with the default 2 MiB and 62 ms with 8 MiB.
const DFA_CACHE_LIMIT: usize = 8 << 20;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegexError {
    #[error("regex is {len} bytes long, at most {MAX_REGEX_LEN} are allowed")]
    TooLong { len: usize },
    #[error("invalid regex: {0}")]
    Invalid(String),
    #[error("unsupported regex syntax, not part of RE2: {0}")]
    Unsupported(String),
}

/// A pattern compiled for matching keys, with the trigram query that selects
/// candidate keys from a regex index.
///
/// The pattern uses RE2 syntax (<https://github.com/google/re2/wiki/Syntax>,
/// see [`super::re2`]), and matches anywhere in a value unless anchored with
/// `^` / `$`. Matching is case-sensitive unless the pattern opts out with
/// `(?i)`.
#[derive(Debug, Clone)]
pub struct RegexMatcher {
    regex: Regex,
    query: TrigramQuery,
}

impl RegexMatcher {
    pub fn new(pattern: &str) -> Result<Self, RegexError> {
        if pattern.len() > MAX_REGEX_LEN {
            return Err(RegexError::TooLong { len: pattern.len() });
        }

        let pattern = to_engine_pattern(pattern)?;

        let regex = RegexBuilder::new(&pattern)
            .size_limit(COMPILED_SIZE_LIMIT)
            .dfa_size_limit(DFA_CACHE_LIMIT)
            .build()
            .map_err(|err| RegexError::Invalid(err.to_string()))?;

        // Parsed with the same (default) syntax options as `regex` uses, so
        // the analysis sees the language the regex matches.
        let hir =
            regex_syntax::parse(&pattern).map_err(|err| RegexError::Invalid(err.to_string()))?;
        let query = TrigramQuery::from_hir(&hir);

        Ok(Self { regex, query })
    }

    pub fn is_match(&self, value: &str) -> bool {
        self.regex.is_match(value)
    }

    /// Query every matching key satisfies.
    pub fn query(&self) -> &TrigramQuery {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_anywhere_unless_anchored() {
        let matcher = RegexMatcher::new("ein").unwrap();
        assert!(matcher.is_match("Albert_Einstein"));
        assert!(
            !RegexMatcher::new("^ein")
                .unwrap()
                .is_match("Albert_Einstein")
        );
        assert!(
            RegexMatcher::new("(?i)^albert")
                .unwrap()
                .is_match("Albert_Einstein")
        );
    }

    /// RE2 semantics: Perl classes and word boundaries are ASCII.
    #[test]
    fn perl_classes_are_ascii() {
        let is_match =
            |pattern: &str, value: &str| RegexMatcher::new(pattern).unwrap().is_match(value);
        assert!(is_match(r"^\d+$", "2024"));
        assert!(!is_match(r"\d", "٣३"));
        assert!(is_match(r"\D", "٣"));
        assert!(!is_match(r"^\w+$", "café"));
        assert!(is_match(r"^[\w é]+$", "café x"));
        assert!(!is_match(r"\s", "\u{a0}\u{b}"));
        // `é` is not an ASCII word character, so there is a boundary before it.
        assert!(is_match(r"caf\b", "café"));
        assert!(!is_match(r"caf\B", "café"));
        // Everything else stays Unicode-aware.
        assert!(is_match(r"^\pL+$", "café"));
        assert!(is_match(r"^.{4}$", "café"));
        assert!(is_match("(?i)CAFÉ", "café"));
    }

    #[test]
    fn rejects_non_re2_patterns() {
        assert!(matches!(
            RegexMatcher::new("[a-z&&[^aeiou]]"),
            Err(RegexError::Unsupported(_))
        ));
        assert!(matches!(
            RegexMatcher::new(r"\p{Emoji}"),
            Err(RegexError::Unsupported(_))
        ));
    }

    #[test]
    fn rejects_bad_patterns() {
        assert!(matches!(
            RegexMatcher::new("(ab"),
            Err(RegexError::Invalid(_))
        ));
        // No backreferences or look-around.
        assert!(matches!(
            RegexMatcher::new(r"(a)\1"),
            Err(RegexError::Invalid(_))
        ));
        assert!(matches!(
            RegexMatcher::new("a(?=b)"),
            Err(RegexError::Invalid(_))
        ));
        assert!(matches!(
            RegexMatcher::new(&"a".repeat(MAX_REGEX_LEN + 1)),
            Err(RegexError::TooLong { .. }),
        ));
        // Too big once compiled.
        assert!(matches!(
            RegexMatcher::new(r"\pL{1000}"),
            Err(RegexError::Invalid(_))
        ));
        // Big, but within limits.
        assert!(RegexMatcher::new(r"\pL{1,100}x").is_ok());
    }
}
