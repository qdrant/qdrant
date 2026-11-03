//! The public regex syntax is RE2's (<https://github.com/google/re2/wiki/Syntax>),
//! not whatever the engine underneath happens to accept.
//!
//! [`to_engine_pattern`] checks a pattern against RE2 and rewrites it into an
//! equivalent pattern for the `regex` crate:
//!
//! - constructs RE2 doesn't have are rejected: class set operations (`&&`,
//!   `--`, `~~`), nested classes, the `u`, `x` and `R` flags, `\u` / `\U`
//!   escapes, `\<`, `\>`, `\b{...}`, Unicode classes other than general
//!   categories and scripts, and counted repetitions above 1000;
//! - `\d`, `\s`, `\w`, `\b` and `\B` are ASCII in RE2 but Unicode-aware in the
//!   `regex` crate, so they are spelled out as their ASCII definitions.
//!
//! RE2 constructs the engine doesn't support are rejected by its parser: `\C`,
//! `\Q...\E` and octal escapes.

use regex_syntax::ast::{
    self, AssertionKind, Ast, ClassPerl, ClassPerlKind, ClassSetBinaryOp, ClassSetItem,
    ClassUnicode, ClassUnicodeKind, Flag, Flags, FlagsItemKind, GroupKind, HexLiteralKind, Literal,
    LiteralKind, RepetitionKind, RepetitionRange, Span,
};

use super::matcher::RegexError;

/// RE2's limit on counted repetitions `x{n,m}`.
const MAX_REPEAT: u32 = 1000;

/// General categories RE2 supports (all but `Cn`, `LC`, `L&`).
const GENERAL_CATEGORIES: &[&str] = &[
    "C", "Cc", "Cf", "Co", "Cs", "L", "Ll", "Lm", "Lo", "Lt", "Lu", "M", "Mc", "Me", "Mn", "N",
    "Nd", "Nl", "No", "P", "Pc", "Pd", "Pe", "Pf", "Pi", "Po", "Ps", "S", "Sc", "Sk", "Sm", "So",
    "Z", "Zl", "Zp", "Zs",
];

/// Scripts RE2 supports, as listed in its syntax reference.
const SCRIPTS: &[&str] = &[
    "Adlam",
    "Ahom",
    "Anatolian_Hieroglyphs",
    "Arabic",
    "Armenian",
    "Avestan",
    "Balinese",
    "Bamum",
    "Bassa_Vah",
    "Batak",
    "Bengali",
    "Bhaiksuki",
    "Bopomofo",
    "Brahmi",
    "Braille",
    "Buginese",
    "Buhid",
    "Canadian_Aboriginal",
    "Carian",
    "Caucasian_Albanian",
    "Chakma",
    "Cham",
    "Cherokee",
    "Chorasmian",
    "Common",
    "Coptic",
    "Cuneiform",
    "Cypriot",
    "Cypro_Minoan",
    "Cyrillic",
    "Deseret",
    "Devanagari",
    "Dives_Akuru",
    "Dogra",
    "Duployan",
    "Egyptian_Hieroglyphs",
    "Elbasan",
    "Elymaic",
    "Ethiopic",
    "Georgian",
    "Glagolitic",
    "Gothic",
    "Grantha",
    "Greek",
    "Gujarati",
    "Gunjala_Gondi",
    "Gurmukhi",
    "Han",
    "Hangul",
    "Hanifi_Rohingya",
    "Hanunoo",
    "Hatran",
    "Hebrew",
    "Hiragana",
    "Imperial_Aramaic",
    "Inherited",
    "Inscriptional_Pahlavi",
    "Inscriptional_Parthian",
    "Javanese",
    "Kaithi",
    "Kannada",
    "Katakana",
    "Kawi",
    "Kayah_Li",
    "Kharoshthi",
    "Khitan_Small_Script",
    "Khmer",
    "Khojki",
    "Khudawadi",
    "Lao",
    "Latin",
    "Lepcha",
    "Limbu",
    "Linear_A",
    "Linear_B",
    "Lisu",
    "Lycian",
    "Lydian",
    "Mahajani",
    "Makasar",
    "Malayalam",
    "Mandaic",
    "Manichaean",
    "Marchen",
    "Masaram_Gondi",
    "Medefaidrin",
    "Meetei_Mayek",
    "Mende_Kikakui",
    "Meroitic_Cursive",
    "Meroitic_Hieroglyphs",
    "Miao",
    "Modi",
    "Mongolian",
    "Mro",
    "Multani",
    "Myanmar",
    "Nabataean",
    "Nag_Mundari",
    "Nandinagari",
    "New_Tai_Lue",
    "Newa",
    "Nko",
    "Nushu",
    "Nyiakeng_Puachue_Hmong",
    "Ogham",
    "Ol_Chiki",
    "Old_Hungarian",
    "Old_Italic",
    "Old_North_Arabian",
    "Old_Permic",
    "Old_Persian",
    "Old_Sogdian",
    "Old_South_Arabian",
    "Old_Turkic",
    "Old_Uyghur",
    "Oriya",
    "Osage",
    "Osmanya",
    "Pahawh_Hmong",
    "Palmyrene",
    "Pau_Cin_Hau",
    "Phags_Pa",
    "Phoenician",
    "Psalter_Pahlavi",
    "Rejang",
    "Runic",
    "Samaritan",
    "Saurashtra",
    "Sharada",
    "Shavian",
    "Siddham",
    "SignWriting",
    "Sinhala",
    "Sogdian",
    "Sora_Sompeng",
    "Soyombo",
    "Sundanese",
    "Syloti_Nagri",
    "Syriac",
    "Tagalog",
    "Tagbanwa",
    "Tai_Le",
    "Tai_Tham",
    "Tai_Viet",
    "Takri",
    "Tamil",
    "Tangsa",
    "Tangut",
    "Telugu",
    "Thaana",
    "Thai",
    "Tibetan",
    "Tifinagh",
    "Tirhuta",
    "Toto",
    "Ugaritic",
    "Vai",
    "Vithkuqi",
    "Wancho",
    "Warang_Citi",
    "Yezidi",
    "Yi",
    "Zanabazar_Square",
];

/// Check that `pattern` is RE2 syntax, and rewrite it into a pattern the
/// `regex` crate matches with RE2 semantics.
pub fn to_engine_pattern(pattern: &str) -> Result<String, RegexError> {
    let ast = ast::parse::Parser::new()
        .parse(pattern)
        .map_err(|err| RegexError::Invalid(err.to_string()))?;
    let replacements = ast::visit(&ast, Re2Visitor::default())?;

    let mut rewritten = String::with_capacity(pattern.len());
    let mut copied = 0;
    for (span, replacement) in replacements {
        rewritten.push_str(&pattern[copied..span.start.offset]);
        rewritten.push_str(replacement);
        copied = span.end.offset;
    }
    rewritten.push_str(&pattern[copied..]);
    Ok(rewritten)
}

/// Collects the ASCII rewrites, in pattern order, and rejects what RE2
/// doesn't support.
#[derive(Default)]
struct Re2Visitor {
    replacements: Vec<(Span, &'static str)>,
}

fn unsupported(what: &str) -> RegexError {
    RegexError::Unsupported(what.to_string())
}

impl Re2Visitor {
    fn check_flags(flags: &Flags) -> Result<(), RegexError> {
        for item in &flags.items {
            match item.kind {
                FlagsItemKind::Negation
                | FlagsItemKind::Flag(
                    Flag::CaseInsensitive
                    | Flag::MultiLine
                    | Flag::DotMatchesNewLine
                    | Flag::SwapGreed,
                ) => {}
                FlagsItemKind::Flag(Flag::Unicode) => return Err(unsupported("flag `u`")),
                FlagsItemKind::Flag(Flag::IgnoreWhitespace) => {
                    return Err(unsupported("flag `x`"));
                }
                FlagsItemKind::Flag(Flag::CRLF) => return Err(unsupported("flag `R`")),
            }
        }
        Ok(())
    }

    fn check_literal(literal: &Literal) -> Result<(), RegexError> {
        match literal.kind {
            LiteralKind::HexFixed(HexLiteralKind::UnicodeShort | HexLiteralKind::UnicodeLong)
            | LiteralKind::HexBrace(HexLiteralKind::UnicodeShort | HexLiteralKind::UnicodeLong) => {
                Err(unsupported(r"`\u` / `\U` escapes (use `\x{...}`)"))
            }
            LiteralKind::Verbatim
            | LiteralKind::Meta
            | LiteralKind::Superfluous
            | LiteralKind::Octal
            | LiteralKind::HexFixed(HexLiteralKind::X)
            | LiteralKind::HexBrace(HexLiteralKind::X)
            | LiteralKind::Special(_) => Ok(()),
        }
    }

    fn check_unicode_class(class: &ClassUnicode) -> Result<(), RegexError> {
        match &class.kind {
            ClassUnicodeKind::OneLetter(_) => Ok(()),
            ClassUnicodeKind::Named(name)
                if GENERAL_CATEGORIES.contains(&name.as_str())
                    || SCRIPTS.contains(&name.as_str()) =>
            {
                Ok(())
            }
            ClassUnicodeKind::Named(name) => Err(RegexError::Unsupported(format!(
                r"Unicode class `\p{{{name}}}` (RE2 has general categories and scripts only)"
            ))),
            ClassUnicodeKind::NamedValue { .. } => {
                Err(unsupported(r"`\p{name=value}` Unicode classes"))
            }
        }
    }

    /// ASCII spelling of a Perl class, as RE2 defines it.
    fn ascii_perl_class(class: &ClassPerl, in_brackets: bool) -> &'static str {
        match (&class.kind, class.negated, in_brackets) {
            (ClassPerlKind::Digit, false, false) => "[0-9]",
            (ClassPerlKind::Digit, true, _) => "[^0-9]",
            (ClassPerlKind::Digit, false, true) => "0-9",
            (ClassPerlKind::Space, false, false) => r"[\t\n\f\r ]",
            (ClassPerlKind::Space, true, _) => r"[^\t\n\f\r ]",
            (ClassPerlKind::Space, false, true) => r"\t\n\f\r ",
            (ClassPerlKind::Word, false, false) => "[0-9A-Za-z_]",
            (ClassPerlKind::Word, true, _) => "[^0-9A-Za-z_]",
            (ClassPerlKind::Word, false, true) => "0-9A-Za-z_",
        }
    }
}

impl ast::Visitor for Re2Visitor {
    type Output = Vec<(Span, &'static str)>;
    type Err = RegexError;

    fn finish(mut self) -> Result<Self::Output, Self::Err> {
        self.replacements.sort_by_key(|(span, _)| span.start.offset);
        Ok(self.replacements)
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Self::Err> {
        match ast {
            Ast::Flags(set_flags) => Self::check_flags(&set_flags.flags)?,
            Ast::Group(group) => match &group.kind {
                GroupKind::NonCapturing(flags) => Self::check_flags(flags)?,
                GroupKind::CaptureIndex(_) | GroupKind::CaptureName { .. } => {}
            },
            Ast::Literal(literal) => Self::check_literal(literal)?,
            Ast::Assertion(assertion) => match assertion.kind {
                AssertionKind::StartLine
                | AssertionKind::EndLine
                | AssertionKind::StartText
                | AssertionKind::EndText => {}
                // ASCII word boundaries, as in RE2.
                AssertionKind::WordBoundary => {
                    self.replacements.push((assertion.span, r"(?-u:\b)"));
                }
                AssertionKind::NotWordBoundary => {
                    self.replacements.push((assertion.span, r"(?-u:\B)"));
                }
                AssertionKind::WordBoundaryStart
                | AssertionKind::WordBoundaryEnd
                | AssertionKind::WordBoundaryStartAngle
                | AssertionKind::WordBoundaryEndAngle
                | AssertionKind::WordBoundaryStartHalf
                | AssertionKind::WordBoundaryEndHalf => {
                    return Err(unsupported(r"`\<`, `\>` and `\b{...}` assertions"));
                }
            },
            Ast::ClassUnicode(class) => Self::check_unicode_class(class)?,
            Ast::ClassPerl(class) => {
                self.replacements
                    .push((class.span, Self::ascii_perl_class(class, false)));
            }
            Ast::Repetition(repetition) => match repetition.op.kind {
                RepetitionKind::Range(
                    RepetitionRange::Exactly(n)
                    | RepetitionRange::AtLeast(n)
                    | RepetitionRange::Bounded(_, n),
                ) if n > MAX_REPEAT => {
                    return Err(RegexError::Unsupported(format!(
                        "repetition count above {MAX_REPEAT}"
                    )));
                }
                RepetitionKind::ZeroOrOne
                | RepetitionKind::ZeroOrMore
                | RepetitionKind::OneOrMore
                | RepetitionKind::Range(_) => {}
            },
            Ast::Empty(_)
            | Ast::Dot(_)
            | Ast::ClassBracketed(_)
            | Ast::Alternation(_)
            | Ast::Concat(_) => {}
        }
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ClassSetItem) -> Result<(), Self::Err> {
        match item {
            ClassSetItem::Literal(literal) => Self::check_literal(literal)?,
            ClassSetItem::Range(range) => {
                Self::check_literal(&range.start)?;
                Self::check_literal(&range.end)?;
            }
            ClassSetItem::Unicode(class) => Self::check_unicode_class(class)?,
            ClassSetItem::Perl(class) => {
                self.replacements
                    .push((class.span, Self::ascii_perl_class(class, true)));
            }
            ClassSetItem::Bracketed(_) => return Err(unsupported("nested character classes")),
            ClassSetItem::Empty(_) | ClassSetItem::Ascii(_) | ClassSetItem::Union(_) => {}
        }
        Ok(())
    }

    fn visit_class_set_binary_op_pre(&mut self, _op: &ClassSetBinaryOp) -> Result<(), Self::Err> {
        Err(unsupported("class set operations (`&&`, `--`, `~~`)"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(pattern: &str) -> String {
        to_engine_pattern(pattern).unwrap()
    }

    fn rejected(pattern: &str) -> bool {
        matches!(to_engine_pattern(pattern), Err(RegexError::Unsupported(_)))
    }

    #[test]
    fn keeps_plain_re2() {
        for pattern in [
            "abc",
            "^a.b$",
            "(?i)ab|cd",
            "(?ms-i:x)",
            "(?U)a+",
            "a{2,1000}",
            r"[[:alpha:]_-]+",
            r"\pL\p{Greek}\P{Lu}\p{Han}",
            r"\x41\x{1F600}\t\n\.\*",
            r"(?P<name>a)(?<other>b)",
            r"\A\z",
        ] {
            assert_eq!(rewritten(pattern), pattern, "{pattern}");
        }
    }

    #[test]
    fn spells_out_ascii_perl_classes() {
        assert_eq!(rewritten(r"\d+"), "[0-9]+");
        assert_eq!(rewritten(r"\W\S"), r"[^0-9A-Za-z_][^\t\n\f\r ]");
        assert_eq!(rewritten(r"[\d_]"), "[0-9_]");
        assert_eq!(rewritten(r"[^\s,]"), r"[^\t\n\f\r ,]");
        assert_eq!(rewritten(r"[\D]"), "[[^0-9]]");
        assert_eq!(rewritten(r"\bword\B"), r"(?-u:\b)word(?-u:\B)");
    }

    #[test]
    fn rejects_non_re2_syntax() {
        for pattern in [
            "[a-z&&[^aeiou]]",
            "[0-9--4]",
            "[a-g~~b-h]",
            "[a[bc]]",
            "(?x) a b",
            "(?u)a",
            "(?R)a",
            r"\u0041",
            r"\U0001F600",
            r"\u{41}",
            r"\<word\>",
            r"\b{start}a",
            r"\p{Alphabetic}",
            r"\p{Script=Greek}",
            r"\p{greek}",
            "a{1001}",
            "a{2,1001}",
        ] {
            assert!(rejected(pattern), "{pattern}");
        }
    }

    #[test]
    fn engine_rejects_unsupported_re2() {
        for pattern in [r"\C", r"\Qa.b\E", r"\123"] {
            assert!(
                matches!(to_engine_pattern(pattern), Err(RegexError::Invalid(_))),
                "{pattern}",
            );
        }
    }
}
