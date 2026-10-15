use std::borrow::Cow;

use super::TokensProcessor;

/// Tokenizer for source code.
///
/// Splits text into identifiers, the way Lucene's `WordDelimiterGraphFilter` does for code search,
/// and then splits each identifier into its subwords:
///
/// - an identifier is a run of alphanumeric characters and `_`; anything else separates them,
/// - `snake_case`, `SCREAMING_CASE`: split on `_`,
/// - `camelCase`, `PascalCase`: split before an uppercase letter that follows a lowercase letter
///   or a digit (`Int32Array` -> `Int32`, `Array`),
/// - acronyms: split before the last uppercase letter of a run when a lowercase letter follows
///   (`HTTPServer` -> `HTTP`, `Server`), except a plural `s` (`getURLs` -> `get`, `URLs`),
/// - digits stay with their letters (`utf8`, `sha256`, `x86`, `k8s`),
/// - a run that starts with a digit is a number literal and is kept whole, without its `_`
///   separators (`1_000_000` -> `1000000`, `0xFF`, `3e10`).
///
/// Documents index the subwords in text order, followed by every multi-part identifier joined into
/// one token (`getUserName` -> `getusername`). Queries are split into subwords only. Together, a
/// query for `getUserName`, `get_user_name`, `GetUserName` or `user name` finds all four
/// spellings, and a query for `getusername` or `hashmap` finds the joined identifier.
///
/// Joined identifiers are detached from the document's token sequence: phrase queries are made
/// of subwords, and the index keeps a joined identifier apart from all other tokens, so it only
/// matches a phrase of itself alone. Phrases match exactly where they would without joined
/// identifiers.
pub struct CodeTokenizer;

impl CodeTokenizer {
    /// Calls `callback` with each token, and whether it is a detached joined identifier. Those
    /// come after all subwords.
    pub fn tokenize_doc<'a, C: FnMut(Cow<'a, str>, bool)>(
        text: &'a str,
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        let processor = PartProcessor::new(tokens_processor);
        let mut joined = Vec::new();

        for_each_word(text, |word, is_ascii| {
            let parts = split_word(word, is_ascii, &processor, &mut |token| {
                callback(token, false)
            });
            if parts > 1
                && let Some(token) = processor.joined(word, is_ascii)
            {
                joined.push(token);
            }
        });

        for token in joined {
            callback(token, true);
        }
    }

    pub fn tokenize_query<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        let processor = PartProcessor::new(tokens_processor);
        for_each_word(text, |word, is_ascii| {
            split_word(word, is_ascii, &processor, &mut callback);
        });
    }
}

/// Calls `f` with each run of alphanumeric characters and `_`, and whether the run is ASCII.
fn for_each_word<'a>(text: &'a str, mut f: impl FnMut(&'a str, bool)) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let mut is_ascii = true;
        while let Some(&byte) = bytes.get(i) {
            if byte.is_ascii() {
                if !(byte.is_ascii_alphanumeric() || byte == b'_') {
                    break;
                }
                i += 1;
            } else {
                let c = char_at(text, i);
                if !c.is_alphanumeric() {
                    break;
                }
                is_ascii = false;
                i += c.len_utf8();
            }
        }

        if i > start {
            f(&text[start..i], is_ascii);
        } else {
            // Skip the separator
            i += char_at(text, i).len_utf8();
        }
    }
}

/// The character starting at byte `i`, which must be a char boundary before the end of `text`.
fn char_at(text: &str, i: usize) -> char {
    text[i..].chars().next().unwrap_or_default()
}

/// Applies [`TokensProcessor`] to subwords, skipping its general path when all it would do to an
/// ASCII subword is lowercase it: whether a subword has uppercase letters is already known from
/// splitting it.
struct PartProcessor<'p> {
    tokens_processor: &'p TokensProcessor,
    /// Processing an ASCII token is at most lowercasing it.
    ascii_lowercase_only: bool,
}

impl<'p> PartProcessor<'p> {
    fn new(tokens_processor: &'p TokensProcessor) -> Self {
        Self {
            tokens_processor,
            ascii_lowercase_only: tokens_processor.is_ascii_lowercase_only(),
        }
    }

    /// An ASCII subword, which has uppercase letters if `has_upper`.
    fn ascii<'a>(&self, part: &'a str, has_upper: bool) -> Option<Cow<'a, str>> {
        if !self.ascii_lowercase_only {
            return self.tokens_processor.process_token(part, true);
        }
        if has_upper && self.tokens_processor.lowercase {
            Some(Cow::Owned(part.to_ascii_lowercase()))
        } else {
            Some(Cow::Borrowed(part))
        }
    }

    /// The word without its `_` separators: all of its subwords, joined.
    fn joined<'a>(&self, word: &'a str, is_ascii: bool) -> Option<Cow<'a, str>> {
        if !(is_ascii && self.ascii_lowercase_only) {
            let joined = if word.contains('_') {
                Cow::Owned(word.chars().filter(|&c| c != '_').collect())
            } else {
                Cow::Borrowed(word)
            };
            return self.tokens_processor.process_token_cow(joined, true);
        }

        let lowercase = self.tokens_processor.lowercase;
        let bytes = word.as_bytes();
        if !bytes
            .iter()
            .any(|&b| b == b'_' || (lowercase && b.is_ascii_uppercase()))
        {
            return Some(Cow::Borrowed(word));
        }

        let mut joined = String::with_capacity(word.len());
        for &byte in bytes {
            if byte != b'_' {
                joined.push(char::from(if lowercase {
                    byte.to_ascii_lowercase()
                } else {
                    byte
                }));
            }
        }
        Some(Cow::Owned(joined))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CharClass {
    Upper,
    /// Lowercase letters, and letters without case.
    Lower,
    Digit,
    Underscore,
}

fn ascii_class(byte: u8) -> CharClass {
    match byte {
        b'A'..=b'Z' => CharClass::Upper,
        b'0'..=b'9' => CharClass::Digit,
        b'_' => CharClass::Underscore,
        _ => CharClass::Lower,
    }
}

fn char_class(c: char) -> CharClass {
    if c.is_ascii() {
        ascii_class(c as u8)
    } else if c.is_uppercase() {
        CharClass::Upper
    } else if c.is_numeric() {
        CharClass::Digit
    } else {
        CharClass::Lower
    }
}

/// Passes each processed subword of `word` to `callback`, in order, and returns how many subwords
/// it has.
fn split_word<'a>(
    word: &'a str,
    is_ascii: bool,
    processor: &PartProcessor,
    callback: &mut impl FnMut(Cow<'a, str>),
) -> usize {
    if word.starts_with(|c: char| c.is_ascii_digit()) {
        // A number literal
        if let Some(token) = processor.joined(word, is_ascii) {
            callback(token);
        }
        return 1;
    }

    let mut parts = 0;
    if is_ascii {
        let bytes = word.as_bytes();
        split_classes(
            bytes.len(),
            |i| ascii_class(bytes[i]),
            |i| bytes[i] == b's',
            |i| i,
            word.len(),
            |start, end, has_upper| {
                parts += 1;
                if let Some(token) = processor.ascii(&word[start..end], has_upper) {
                    callback(token);
                }
            },
        );
    } else {
        let chars: Vec<(usize, char)> = word.char_indices().collect();
        split_classes(
            chars.len(),
            |i| char_class(chars[i].1),
            |i| chars[i].1 == 's',
            |i| chars[i].0,
            word.len(),
            |start, end, _| {
                parts += 1;
                let part = &word[start..end];
                if let Some(token) = processor.tokens_processor.process_token(part, true) {
                    callback(token);
                }
            },
        );
    }
    parts
}

/// Finds subword boundaries in a word of `len` characters, and calls `emit` with the byte range of
/// each subword and whether it has uppercase letters. `offset` maps a character index to its byte
/// offset in the word.
fn split_classes(
    len: usize,
    class: impl Fn(usize) -> CharClass,
    is_s: impl Fn(usize) -> bool,
    offset: impl Fn(usize) -> usize,
    byte_len: usize,
    mut emit: impl FnMut(usize, usize, bool),
) {
    // Start of the current subword; there is none while `prev` is an underscore
    let mut start = 0;
    let mut has_upper = false;
    let mut prev = CharClass::Underscore;

    for i in 0..len {
        let cur = class(i);
        match (prev, cur) {
            (CharClass::Underscore, CharClass::Underscore) => {}
            (_, CharClass::Underscore) => emit(offset(start), offset(i), has_upper),
            (CharClass::Underscore, _) => {
                start = i;
                has_upper = false;
            }
            (prev, cur) => {
                let boundary = match (prev, cur) {
                    // camelCase, Int32Array
                    (CharClass::Lower | CharClass::Digit, CharClass::Upper) => true,
                    // HTTPServer, but not URLs or URLsFor
                    (CharClass::Upper, CharClass::Upper) => {
                        i + 1 < len
                            && class(i + 1) == CharClass::Lower
                            && !(is_s(i + 1) && (i + 2 == len || class(i + 2) != CharClass::Lower))
                    }
                    _ => false,
                };
                if boundary {
                    emit(offset(start), offset(i), has_upper);
                    start = i;
                    has_upper = false;
                }
            }
        }
        has_upper |= cur == CharClass::Upper;
        prev = cur;
    }

    if prev != CharClass::Underscore {
        emit(offset(start), byte_len, has_upper);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        CodeTokenizer::tokenize_doc(text, &lowercase(), |t, _| tokens.push(t.into_owned()));
        tokens
    }

    fn query(text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        CodeTokenizer::tokenize_query(text, &lowercase(), |t| tokens.push(t.into_owned()));
        tokens
    }

    fn lowercase() -> TokensProcessor {
        TokensProcessor::new(true, false, Default::default(), None, None, None)
    }

    fn parts(word: &str) -> Vec<String> {
        // A query is its subwords; without lowercasing they keep their case
        let mut parts = Vec::new();
        CodeTokenizer::tokenize_query(word, &TokensProcessor::default(), |p| {
            parts.push(p.into_owned())
        });
        parts
    }

    #[test]
    fn test_split_identifiers() {
        let cases: &[(&str, &[&str])] = &[
            ("getUserName", &["get", "User", "Name"]),
            ("GetUserName", &["Get", "User", "Name"]),
            ("get_user_name", &["get", "user", "name"]),
            ("MAX_BUFFER_SIZE", &["MAX", "BUFFER", "SIZE"]),
            ("__init__", &["init"]),
            ("_private", &["private"]),
            ("HTTPServer", &["HTTP", "Server"]),
            ("XMLHttpRequest", &["XML", "Http", "Request"]),
            ("getHTTPResponseCode", &["get", "HTTP", "Response", "Code"]),
            ("IOError", &["IO", "Error"]),
            ("ASet", &["A", "Set"]),
            ("iOS", &["i", "OS"]),
            ("getURLs", &["get", "URLs"]),
            ("parseURLsFrom", &["parse", "URLs", "From"]),
            ("URLs", &["URLs"]),
            ("TCPsocket", &["TC", "Psocket"]),
            ("Int32Array", &["Int32", "Array"]),
            ("UTF8String", &["UTF8", "String"]),
            ("base64Encode", &["base64", "Encode"]),
            ("utf8", &["utf8"]),
            ("x86_64", &["x86", "64"]),
            ("k8s", &["k8s"]),
            ("1_000_000", &["1000000"]),
            ("0xFF00", &["0xFF00"]),
            ("3DPoint", &["3DPoint"]),
            ("ÜberCafé", &["Über", "Café"]),
            ("значениеПоУмолчанию", &["значение", "По", "Умолчанию"]),
            ("数据Model", &["数据", "Model"]),
            ("_", &[]),
        ];
        for (word, expected) in cases {
            assert_eq!(parts(word), *expected, "splitting {word:?}");
        }
    }

    #[test]
    fn test_document_and_query() {
        let text = "let map: HashMap<String, u32> = self.get_user_name(user_id)?;";
        assert_eq!(
            doc(text),
            [
                "let",
                "map",
                "hash",
                "map",
                "string",
                "u32",
                "self",
                "get",
                "user",
                "name",
                "user",
                "id",
                "hashmap",
                "getusername",
                "userid",
            ],
        );
        assert_eq!(
            query(text),
            [
                "let", "map", "hash", "map", "string", "u32", "self", "get", "user", "name",
                "user", "id",
            ],
        );
    }

    #[test]
    fn test_spellings_match() {
        // Every query token is in the document, whatever the convention of either
        let spellings = [
            "getUserName",
            "GetUserName",
            "get_user_name",
            "GET_USER_NAME",
            "get-user-name",
            "get user name",
        ];
        for document in spellings {
            let doc_tokens = doc(document);
            for q in spellings {
                for token in query(q) {
                    assert!(doc_tokens.contains(&token), "{q:?} -> {document:?}");
                }
            }
        }

        // A query word that does not split finds the joined identifier
        assert!(doc("new HashMap()").contains(&query("hashmap")[0]));
        assert!(doc("get_user_name").contains(&query("getusername")[0]));
    }

    #[test]
    fn test_fast_path_matches_general_path() {
        // `min_token_len: Some(1)` drops nothing, but disables the fast path
        let text = "fn getHTTPResponse(MAX_SIZE: u32, url_list: &[URLs]) -> 0xFF_FF { ÜberCafé_x }";
        for lowercase in [true, false] {
            let fast = TokensProcessor::new(lowercase, false, Default::default(), None, None, None);
            let general =
                TokensProcessor::new(lowercase, false, Default::default(), None, Some(1), None);
            assert!(fast.is_ascii_lowercase_only());
            assert!(!general.is_ascii_lowercase_only());

            let mut fast_tokens = Vec::new();
            CodeTokenizer::tokenize_doc(text, &fast, |t, _| fast_tokens.push(t));
            let mut general_tokens = Vec::new();
            CodeTokenizer::tokenize_doc(text, &general, |t, _| general_tokens.push(t));
            assert_eq!(fast_tokens, general_tokens, "lowercase: {lowercase}");
        }
    }
}
