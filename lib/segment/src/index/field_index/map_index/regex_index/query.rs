//! Derive a [`TrigramQuery`] from a regular expression: a boolean query over
//! trigrams that every key matching the expression satisfies.
//!
//! The analysis follows Russ Cox, "Regular Expression Matching with a Trigram
//! Index" (<https://swtch.com/~rsc/regexp/regexp4.html>), as implemented in
//! Google Code Search. For every node of the expression it tracks:
//!
//! - `can_empty`: whether the node matches the empty string;
//! - `exact`: the full set of strings the node matches, if small and known;
//! - `prefix` / `suffix`: sets that every match starts / ends with, used when
//!   `exact` is unknown;
//! - `query`: a trigram query every match satisfies.
//!
//! Information moves from the sets into `query` as the sets grow, so the sets
//! stay small. Everything is computed on [folded](super::trigram::fold) bytes,
//! and the text anchors `^` / `$` contribute the
//! [`BOUNDARY`](super::trigram::BOUNDARY) byte the keys are wrapped in.
//!
//! The query is *sound*: it may admit keys that don't match (they are
//! verified with the regex afterwards), but never rejects a key that does.
//! Patterns without a required 3-byte substring (`^.{3}$`, `\d+`) yield
//! [`TrigramQuery::All`]: they are answered by checking every key.

use regex_syntax::hir::{Class, Hir, HirKind, Look};

use super::trigram::{BOUNDARY, Trigram, fold, windows};

/// Exact sets above this size are turned into trigrams and prefix/suffix sets.
const MAX_EXACT: usize = 7;
/// Prefix/suffix sets above this size are truncated to shorter strings.
const MAX_SET: usize = 20;
/// Character classes larger than this are treated as "any character".
const MAX_CLASS: usize = 100;
/// Repetitions `x{n}` with `n` up to this are expanded into a concatenation.
const MAX_EXPANDED_REPEAT: u32 = 4;
/// Upper bound of trigrams in the final query; each one is a postings read.
pub const MAX_QUERY_TRIGRAMS: usize = 64;

/// Boolean query over trigrams of a key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrigramQuery {
    /// Every key is a candidate.
    All,
    /// No key can match.
    None,
    Trigram(Trigram),
    And(Vec<TrigramQuery>),
    Or(Vec<TrigramQuery>),
}

impl TrigramQuery {
    /// Query satisfied by every key the expression `hir` matches.
    pub fn from_hir(hir: &Hir) -> Self {
        let mut info = analyze(hir);
        info.simplify(true);
        info.add_exact();
        let mut budget = MAX_QUERY_TRIGRAMS;
        info.query.prune(&mut budget)
    }

    /// Number of trigram leaves.
    pub fn trigram_count(&self) -> usize {
        match self {
            Self::All | Self::None => 0,
            Self::Trigram(_) => 1,
            Self::And(children) | Self::Or(children) => {
                children.iter().map(Self::trigram_count).sum()
            }
        }
    }

    /// Evaluate the query against the trigrams of a single key.
    pub fn matches(&self, has_trigram: &impl Fn(Trigram) -> bool) -> bool {
        match self {
            Self::All => true,
            Self::None => false,
            Self::Trigram(trigram) => has_trigram(*trigram),
            Self::And(children) => children.iter().all(|child| child.matches(has_trigram)),
            Self::Or(children) => children.iter().any(|child| child.matches(has_trigram)),
        }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, query) | (query, Self::All) => query,
            (a, b) => {
                let mut children = Vec::new();
                for query in [a, b] {
                    match query {
                        Self::And(nested) => children.extend(nested),
                        query @ (Self::All | Self::None | Self::Trigram(_) | Self::Or(_)) => {
                            children.push(query)
                        }
                    }
                }
                children.sort_unstable();
                children.dedup();
                Self::And(children)
            }
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::None, query) | (query, Self::None) => query,
            (a, b) => {
                let mut children = Vec::new();
                for query in [a, b] {
                    match query {
                        Self::Or(nested) => children.extend(nested),
                        query @ (Self::All | Self::None | Self::Trigram(_) | Self::And(_)) => {
                            children.push(query)
                        }
                    }
                }
                children.sort_unstable();
                children.dedup();
                if children.len() == 1 {
                    children.pop().unwrap()
                } else {
                    Self::Or(children)
                }
            }
        }
    }

    /// `self AND (OR over s in set of (AND of trigrams of s))`; no-op if some
    /// string in the set is too short to contain a trigram.
    fn and_trigrams(self, set: &StringSet) -> Self {
        if set.min_len() < 3 {
            return self;
        }
        let mut alternatives = Self::None;
        for string in &set.0 {
            let mut trigrams: Vec<Trigram> = windows(string).collect();
            trigrams.sort_unstable();
            trigrams.dedup();
            let conjunction = match trigrams.as_slice() {
                [trigram] => Self::Trigram(*trigram),
                _ => Self::And(trigrams.into_iter().map(Self::Trigram).collect()),
            };
            alternatives = alternatives.or(conjunction);
        }
        self.and(alternatives)
    }

    /// Keep at most `budget` trigrams. Dropping a conjunct only widens the
    /// query, so conjunctions keep what fits; a disjunction that doesn't fit
    /// whole becomes [`Self::All`].
    fn prune(self, budget: &mut usize) -> Self {
        match self {
            Self::All | Self::None => self,
            Self::Trigram(_) => {
                if *budget == 0 {
                    Self::All
                } else {
                    *budget -= 1;
                    self
                }
            }
            Self::And(mut children) => {
                // Cheapest first, so the budget keeps as many conjuncts as possible.
                children.sort_by_key(Self::trigram_count);
                children
                    .into_iter()
                    .fold(Self::All, |acc, child| acc.and(child.prune(budget)))
            }
            Self::Or(children) => {
                let count: usize = children.iter().map(Self::trigram_count).sum();
                if count > *budget {
                    Self::All
                } else {
                    *budget -= count;
                    Self::Or(children)
                }
            }
        }
    }
}

type Bytes = Vec<u8>;

/// Set of byte strings, sorted (by prefix or by suffix order) and deduplicated.
#[derive(Debug, Clone, Default)]
struct StringSet(Vec<Bytes>);

impl StringSet {
    fn single(string: Bytes) -> Self {
        Self(vec![string])
    }

    fn empty_string() -> Self {
        Self::single(Bytes::new())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn min_len(&self) -> usize {
        self.0.iter().map(Vec::len).min().unwrap_or(0)
    }

    /// Sort and deduplicate. Suffix sets are ordered by reversed strings, so
    /// that strings sharing a suffix are adjacent.
    fn clean(&mut self, is_suffix: bool) {
        if is_suffix {
            self.0
                .sort_unstable_by(|a, b| a.iter().rev().cmp(b.iter().rev()));
        } else {
            self.0.sort_unstable();
        }
        self.0.dedup();
    }

    fn union(mut self, other: &Self, is_suffix: bool) -> Self {
        self.0.extend_from_slice(&other.0);
        self.clean(is_suffix);
        self
    }

    /// All concatenations `a + b`. Anchors are zero-width, so adjacent
    /// boundary markers (`^^`) collapse into one, as in the wrapped key.
    fn cross(&self, other: &Self, is_suffix: bool) -> Self {
        let mut product = Vec::with_capacity(self.len() * other.len());
        for a in &self.0 {
            for b in &other.0 {
                let mut string = a.clone();
                match (a.last(), b.first()) {
                    (Some(&BOUNDARY), Some(&BOUNDARY)) => string.extend_from_slice(&b[1..]),
                    _ => string.extend_from_slice(b),
                }
                product.push(string);
            }
        }
        let mut set = Self(product);
        set.clean(is_suffix);
        set
    }
}

/// What the analysis knows about one node of the expression.
#[derive(Debug, Clone)]
struct Info {
    can_empty: bool,
    /// All strings the node matches; `None` when unknown or too many.
    exact: Option<StringSet>,
    /// Every match starts with one of these; meaningful when `exact` is unknown.
    prefix: StringSet,
    /// Every match ends with one of these; meaningful when `exact` is unknown.
    suffix: StringSet,
    query: TrigramQuery,
}

impl Info {
    /// Matches any string, including the empty one (`x*`).
    fn any_match() -> Self {
        Self {
            can_empty: true,
            exact: None,
            prefix: StringSet::empty_string(),
            suffix: StringSet::empty_string(),
            query: TrigramQuery::All,
        }
    }

    /// Matches some non-empty string we know nothing about (`.`).
    fn any_char() -> Self {
        Self {
            can_empty: false,
            ..Self::any_match()
        }
    }

    fn no_match() -> Self {
        Self {
            can_empty: false,
            exact: None,
            prefix: StringSet::default(),
            suffix: StringSet::default(),
            query: TrigramQuery::None,
        }
    }

    fn exact(set: StringSet) -> Self {
        Self {
            can_empty: set.0.iter().any(Vec::is_empty),
            exact: Some(set),
            prefix: StringSet::default(),
            suffix: StringSet::default(),
            query: TrigramQuery::All,
        }
    }

    fn empty_string() -> Self {
        Self::exact(StringSet::empty_string())
    }

    /// Move the exact set's trigrams into the query.
    fn add_exact(&mut self) {
        if let Some(exact) = &self.exact {
            self.query = std::mem::replace(&mut self.query, TrigramQuery::All).and_trigrams(exact);
        }
    }

    /// Keep the sets small, moving their information into the query.
    fn simplify(&mut self, force: bool) {
        let too_big = self.exact.as_mut().is_some_and(|exact| {
            exact.clean(false);
            let min_len = exact.min_len();
            exact.len() > MAX_EXACT || (min_len >= 3 && force) || min_len >= 4
        });
        if too_big {
            self.add_exact();
            if let Some(exact) = self.exact.take() {
                for string in exact.0 {
                    let len = string.len();
                    if len < 3 {
                        self.prefix.0.push(string.clone());
                        self.suffix.0.push(string);
                    } else {
                        self.prefix.0.push(string[..2].to_vec());
                        self.suffix.0.push(string[len - 2..].to_vec());
                    }
                }
            }
        }

        if self.exact.is_none() {
            self.simplify_set(false);
            self.simplify_set(true);
        }
    }

    fn simplify_set(&mut self, is_suffix: bool) {
        let set = if is_suffix {
            &mut self.suffix
        } else {
            &mut self.prefix
        };
        set.clean(is_suffix);

        // The set's own trigrams are required, record them before truncating.
        self.query = std::mem::replace(&mut self.query, TrigramQuery::All).and_trigrams(set);

        // Keep at most 2 bytes per string (a trigram needs 1 more byte from a
        // neighbour), and fewer if the set is still too big.
        let mut n = 3;
        while n == 3 || set.len() > MAX_SET {
            for string in &mut set.0 {
                if string.len() >= n {
                    if is_suffix {
                        string.drain(..string.len() - (n - 1));
                    } else {
                        string.truncate(n - 1);
                    }
                }
            }
            set.clean(is_suffix);
            if n == 1 {
                break;
            }
            n -= 1;
        }

        // A prefix that extends another prefix adds nothing: drop it.
        let mut kept: Vec<Bytes> = Vec::with_capacity(set.len());
        for string in std::mem::take(&mut set.0) {
            let redundant = kept.last().is_some_and(|last| {
                if is_suffix {
                    string.ends_with(last)
                } else {
                    string.starts_with(last)
                }
            });
            if !redundant {
                kept.push(string);
            }
        }
        set.0 = kept;
    }
}

fn concat(x: Info, y: Info) -> Info {
    let Info {
        can_empty: x_can_empty,
        exact: x_exact,
        prefix: x_prefix,
        suffix: x_suffix,
        query: x_query,
    } = x;
    let Info {
        can_empty: y_can_empty,
        exact: y_exact,
        prefix: y_prefix,
        suffix: y_suffix,
        query: y_query,
    } = y;

    let mut xy = Info {
        can_empty: x_can_empty && y_can_empty,
        exact: None,
        prefix: StringSet::default(),
        suffix: StringSet::default(),
        query: x_query.and(y_query),
    };

    match (&x_exact, &y_exact) {
        (Some(x_exact), Some(y_exact)) => xy.exact = Some(x_exact.cross(y_exact, false)),
        _ => {
            xy.prefix = match &x_exact {
                Some(x_exact) => x_exact.cross(&y_prefix, false),
                None if x_can_empty => x_prefix.clone().union(&y_prefix, false),
                None => x_prefix.clone(),
            };
            xy.suffix = match &y_exact {
                Some(y_exact) => x_suffix.cross(y_exact, true),
                None if y_can_empty => y_suffix.clone().union(&x_suffix, true),
                None => y_suffix.clone(),
            };
        }
    }

    // A trigram may span the boundary between `x` and `y`; neither side's
    // sets account for it.
    if x_exact.is_none()
        && y_exact.is_none()
        && x_suffix.len() <= MAX_SET
        && y_prefix.len() <= MAX_SET
        && x_suffix.min_len() + y_prefix.min_len() >= 3
    {
        xy.query = xy.query.and_trigrams(&x_suffix.cross(&y_prefix, false));
    }

    xy.simplify(false);
    xy
}

fn alternate(mut x: Info, mut y: Info) -> Info {
    let (exact, prefix, suffix) = match (x.exact.take(), y.exact.take()) {
        (Some(x_exact), Some(y_exact)) => (
            Some(x_exact.union(&y_exact, false)),
            StringSet::default(),
            StringSet::default(),
        ),
        (Some(x_exact), None) => {
            let prefix = x_exact.clone().union(&y.prefix, false);
            let suffix = x_exact.clone().union(&y.suffix, true);
            x.exact = Some(x_exact);
            x.add_exact();
            (None, prefix, suffix)
        }
        (None, Some(y_exact)) => {
            let prefix = x.prefix.clone().union(&y_exact, false);
            let suffix = x.suffix.clone().union(&y_exact, true);
            y.exact = Some(y_exact);
            y.add_exact();
            (None, prefix, suffix)
        }
        (None, None) => (
            None,
            x.prefix.clone().union(&y.prefix, false),
            x.suffix.clone().union(&y.suffix, true),
        ),
    };

    let mut xy = Info {
        can_empty: x.can_empty || y.can_empty,
        exact,
        prefix,
        suffix,
        query: x.query.or(y.query),
    };
    xy.simplify(false);
    xy
}

/// Matches of a literal: its folded bytes. A byte literal that isn't UTF-8
/// (only possible with Unicode mode disabled) can't be folded consistently
/// with the keys, so nothing is assumed about it.
fn literal(bytes: &[u8]) -> Info {
    match std::str::from_utf8(bytes) {
        Ok(text) => Info::exact(StringSet::single(fold(text).into_bytes())),
        Err(_) => Info::any_char(),
    }
}

fn class(class: &Class) -> Info {
    let strings: Vec<Bytes> = match class {
        Class::Unicode(class) => {
            let size: usize = class
                .ranges()
                .iter()
                .map(|range| range.end() as usize - range.start() as usize + 1)
                .sum();
            if size == 0 {
                return Info::no_match();
            }
            if size > MAX_CLASS {
                return Info::any_char();
            }
            class
                .ranges()
                .iter()
                .flat_map(|range| range.start()..=range.end())
                .map(|char| fold(char.encode_utf8(&mut [0; 4])).into_bytes())
                .collect()
        }
        Class::Bytes(class) => {
            let size: usize = class
                .ranges()
                .iter()
                .map(|range| usize::from(range.end() - range.start()) + 1)
                .sum();
            if size == 0 {
                return Info::no_match();
            }
            // Non-ASCII bytes are fragments of UTF-8 sequences: see `literal`.
            if size > MAX_CLASS || class.ranges().iter().any(|range| !range.end().is_ascii()) {
                return Info::any_char();
            }
            class
                .ranges()
                .iter()
                .flat_map(|range| range.start()..=range.end())
                .map(|byte| vec![byte.to_ascii_lowercase()])
                .collect()
        }
    };
    let mut set = StringSet(strings);
    set.clean(false);
    Info::exact(set)
}

fn analyze(hir: &Hir) -> Info {
    let mut info = match hir.kind() {
        HirKind::Empty => Info::empty_string(),
        HirKind::Literal(lit) => literal(&lit.0),
        HirKind::Class(c) => class(c),
        // Only the text anchors pin a match to the key boundary; line anchors
        // and word boundaries match empty strings anywhere.
        HirKind::Look(Look::Start | Look::End) => Info::exact(StringSet::single(vec![BOUNDARY])),
        HirKind::Look(_) => Info::empty_string(),
        HirKind::Capture(capture) => analyze(&capture.sub),
        HirKind::Repetition(rep) => match (rep.min, rep.max) {
            (0, Some(1)) => alternate(analyze(&rep.sub), Info::empty_string()),
            (0, _) => Info::any_match(),
            (min, Some(max)) if min == max && min <= MAX_EXPANDED_REPEAT => {
                let sub = analyze(&rep.sub);
                (1..min).fold(sub.clone(), |acc, _| concat(acc, sub.clone()))
            }
            (_, _) => {
                // `x{n,}` matches at least one `x`: whatever `x` requires.
                let mut info = analyze(&rep.sub);
                if let Some(exact) = info.exact.take() {
                    info.prefix = exact.clone();
                    info.suffix = exact;
                }
                info
            }
        },
        HirKind::Concat(subs) => subs
            .iter()
            .fold(Info::empty_string(), |acc, sub| concat(acc, analyze(sub))),
        HirKind::Alternation(subs) => {
            let mut subs = subs.iter();
            let first = subs.next().map_or_else(Info::no_match, analyze);
            subs.fold(first, |acc, sub| alternate(acc, analyze(sub)))
        }
    };
    info.simplify(false);
    info
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::trigram::key_trigrams;
    use super::*;

    fn query(pattern: &str) -> TrigramQuery {
        TrigramQuery::from_hir(&regex_syntax::parse(pattern).unwrap())
    }

    fn tri(bytes: &[u8]) -> TrigramQuery {
        TrigramQuery::Trigram(Trigram::new(bytes.try_into().unwrap()))
    }

    fn and(children: Vec<TrigramQuery>) -> TrigramQuery {
        children
            .into_iter()
            .fold(TrigramQuery::All, TrigramQuery::and)
    }

    /// Whether the query of `pattern` lets `key` through as a candidate.
    fn admits(pattern: &str, key: &str) -> bool {
        let trigrams: HashSet<_> = key_trigrams(key).into_iter().collect();
        query(pattern).matches(&|trigram| trigrams.contains(&trigram))
    }

    #[test]
    fn literal_requires_its_trigrams() {
        assert_eq!(query("abcd"), and(vec![tri(b"abc"), tri(b"bcd")]));
        assert_eq!(query("abc"), tri(b"abc"));
        // Case only matters to the regex, not to the trigrams.
        assert_eq!(query("(?i)aBc"), tri(b"abc"));
        assert_eq!(query("ABC"), tri(b"abc"));
    }

    #[test]
    fn short_or_missing_literals_match_all() {
        for pattern in ["", "ab", "a.b", "^.{3}$", r"\d{3}-\d{4}", "[a-z]+", ".*"] {
            assert_eq!(query(pattern), TrigramQuery::All, "{pattern}");
        }
    }

    #[test]
    fn anchors_add_boundary_trigrams() {
        assert_eq!(query("^ab"), tri(b"\xFFab"));
        assert_eq!(query("ab$"), tri(b"ab\xFF"));
        assert_eq!(query("^a$"), tri(b"\xFFa\xFF"));
        // Line anchors and word boundaries are not key boundaries.
        assert_eq!(query("(?m)^ab"), TrigramQuery::All);
        assert_eq!(query(r"\bab\b"), TrigramQuery::All);
    }

    #[test]
    fn leading_and_trailing_wildcards_keep_literals() {
        assert_eq!(query(".*abcd.*"), query("abcd"));
        assert_eq!(query("^.*abc"), tri(b"abc"));

        let pattern = r"\.(dll|exe)$";
        assert!(admits(pattern, "setup.exe"));
        assert!(admits(pattern, "x.dll"));
        assert!(!admits(pattern, "setup.exe.bak"));
        assert!(!admits(pattern, "dll"));
    }

    #[test]
    fn alternation_and_optional() {
        assert_eq!(
            query("abc|def"),
            TrigramQuery::Or(vec![tri(b"abc"), tri(b"def")])
        );
        // An alternative without trigrams makes the whole alternation unconstrained.
        assert_eq!(query("abc|de"), TrigramQuery::All);

        assert!(admits("colou?r", "color"));
        assert!(admits("colou?r", "colour"));
        assert!(!admits("colou?r", "colo"));
        assert!(!admits("colou?r", "olour"));

        assert!(admits("(ab|cd)efg", "abefg"));
        assert!(admits("(ab|cd)efg", "cdefg"));
        // The trigrams spanning `ab|cd` and `efg` are required too.
        assert!(!admits("(ab|cd)efg", "efg_ab_cd"));
    }

    #[test]
    fn repetitions() {
        // `x+` requires `x`.
        assert_eq!(query("(abc)+"), tri(b"abc"));
        // `x*` requires nothing.
        assert_eq!(query("x(abc)*y"), TrigramQuery::All);
        // Small fixed counts expand.
        assert_eq!(query("a{3}"), tri(b"aaa"));
        assert_eq!(query("(ab){2}"), and(vec![tri(b"aba"), tri(b"bab")]));
    }

    #[test]
    fn trigrams_across_unknown_middle() {
        let pattern = "year_[0-9]{4}_in_sports";
        assert!(admits(pattern, "year_2020_in_sports"));
        assert!(!admits(pattern, "year_2020_in_music"));
        assert!(!admits(pattern, "2020_in_sports"));
    }

    #[test]
    fn impossible_patterns_match_nothing() {
        assert_eq!(query("[^\\x00-\\x{10FFFF}]"), TrigramQuery::None);
        assert_eq!(query("abc[^\\x00-\\x{10FFFF}]"), TrigramQuery::None);
    }

    /// The `regex` crate rejects str patterns that can match invalid UTF-8, but
    /// the analysis stays sound for them anyway.
    #[test]
    fn non_utf8_literals_are_unconstrained() {
        let query = |pattern: &str| {
            let hir = regex_syntax::ParserBuilder::new()
                .utf8(false)
                .build()
                .parse(pattern)
                .unwrap();
            TrigramQuery::from_hir(&hir)
        };
        // Adjacent literals merge into one, which then isn't UTF-8 as a whole.
        assert_eq!(query(r"(?-u)\xFFabc"), TrigramQuery::All);
        assert_eq!(query(r"(?-u)[\x80-\xFF]bc"), TrigramQuery::All);
        assert_eq!(query(r"(?-u)[\x80-\xFF]+abc"), tri(b"abc"));
    }

    #[test]
    fn query_size_is_bounded() {
        let pattern = (0..40)
            .map(|i| format!("w{i:03}rd"))
            .collect::<Vec<_>>()
            .join("|");
        assert!(query(&pattern).trigram_count() <= MAX_QUERY_TRIGRAMS);

        let long = "abcdefghijklmnopqrstuvwxyz".repeat(5);
        let q = query(&long);
        assert!(q.trigram_count() <= MAX_QUERY_TRIGRAMS);
        assert_ne!(q, TrigramQuery::All);
    }
}
