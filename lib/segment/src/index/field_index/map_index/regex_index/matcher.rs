//! [`RegexMatcher`]: a user pattern compiled once per query, with the limits
//! that make untrusted patterns safe to run.

use regex::{Regex, RegexBuilder};

use super::query::TrigramQuery;

/// Longest accepted pattern, in bytes.
pub const MAX_REGEX_LEN: usize = 1024;

/// Limit of the compiled program. Matching time is linear in the program
/// size, so this bounds the cost per checked value; Unicode classes under
/// counted repetition (`\w{1000}`) are what hits it.
const COMPILED_SIZE_LIMIT: usize = 10 << 20;

/// Cache of the lazy DFA. Too small a cache makes the engine fall back to a
/// much slower one on Unicode-heavy patterns: `\w{1,100}x` over 1M keys took
/// 3.9 s with the default 2 MiB and 62 ms with 8 MiB.
const DFA_CACHE_LIMIT: usize = 8 << 20;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegexError {
    #[error("regex is {len} bytes long, at most {MAX_REGEX_LEN} are allowed")]
    TooLong { len: usize },
    #[error("invalid regex: {0}")]
    Invalid(String),
}

/// A pattern compiled for matching keys, with the trigram query that selects
/// candidate keys from a regex index.
///
/// The pattern uses the syntax of the `regex` crate (no look-around, no
/// backreferences), and matches anywhere in a value unless anchored with `^`
/// / `$`. Matching is case-sensitive unless the pattern opts out with `(?i)`.
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

        let regex = RegexBuilder::new(pattern)
            .size_limit(COMPILED_SIZE_LIMIT)
            .dfa_size_limit(DFA_CACHE_LIMIT)
            .build()
            .map_err(|err| RegexError::Invalid(err.to_string()))?;

        // Parsed with the same (default) syntax options as `regex` uses, so
        // the analysis sees the language the regex matches.
        let hir =
            regex_syntax::parse(pattern).map_err(|err| RegexError::Invalid(err.to_string()))?;
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

    pub fn as_str(&self) -> &str {
        self.regex.as_str()
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
            RegexMatcher::new(r"\w{1000}"),
            Err(RegexError::Invalid(_))
        ));
        // Big, but within limits.
        assert!(RegexMatcher::new(r"\w{1,100}x").is_ok());
    }
}
