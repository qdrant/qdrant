use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, digit1, none_of, satisfy};
use nom::combinator::{all_consuming, map_res, recognize};
use nom::multi::{many0, many1};
use nom::sequence::{delimited, preceded};
use nom::{IResult, Parser};

use super::v2::{JsonPathItem, JsonPathV2};

impl FromStr for JsonPathV2 {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match json_path(s) {
            Ok(("", path)) => Ok(path),
            _ => Err(()),
        }
    }
}

pub fn key_needs_quoting(s: &str) -> bool {
    let mut parser = all_consuming(raw_str);
    parser(s).is_err()
}

fn json_path(input: &str) -> IResult<&str, JsonPathV2> {
    let (input, first_key) = alt((raw_str.map(str::to_string), quoted_str)).parse(input)?;

    let (input, rest) = many0(alt((
        (preceded(char('.'), raw_str).map(|s| JsonPathItem::Key(s.to_string()))),
        (preceded(char('.'), quoted_str).map(JsonPathItem::Key)),
        (delimited(char('['), number, char(']')).map(JsonPathItem::Index)),
        (tag("[]").map(|_| JsonPathItem::WildcardIndex)),
    )))(input)?;

    Ok((input, JsonPathV2 { first_key, rest }))
}

fn raw_str(input: &str) -> IResult<&str, &str> {
    recognize(many1(
        satisfy(|c: char| c.is_alphanumeric() || c == '_' || c == '-').map(|_: char| ()),
    ))
    .parse(input)
}

fn quoted_str(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let (input, rest) = many0(none_of("\\\""))(input)?;
    let (input, _) = char('"')(input)?;
    Ok((input, rest.iter().collect()))
}

fn number(input: &str) -> IResult<&str, usize> {
    map_res(recognize(digit1), str::parse)(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert!("".parse::<JsonPathV2>().is_err());

        assert_eq!(
            "foo".parse(),
            Ok(JsonPathV2 {
                first_key: "foo".to_string(),
                rest: vec![],
            })
        );

        assert_eq!(
            "foo[1][50].bar-baz[].\"qux[.]quux\"".parse(),
            Ok(JsonPathV2 {
                first_key: "foo".to_string(),
                rest: vec![
                    JsonPathItem::Index(1),
                    JsonPathItem::Index(50),
                    JsonPathItem::Key("bar-baz".to_string()),
                    JsonPathItem::WildcardIndex,
                    JsonPathItem::Key("qux[.]quux".to_string()),
                ],
            })
        );
    }

    #[test]
    fn test_key_needs_quoting() {
        // Key needs no quoting
        assert!(!key_needs_quoting("f"));
        assert!(!key_needs_quoting("foo"));
        assert!(!key_needs_quoting("foo_123-bar"));

        // Key needs quoting
        assert!(key_needs_quoting(""));
        assert!(key_needs_quoting(" foo"));
        assert!(key_needs_quoting("foo "));
        assert!(key_needs_quoting("foo bar"));
        assert!(key_needs_quoting("foo bar baz"));
        assert!(key_needs_quoting("foo.bar.baz"));
        assert!(key_needs_quoting("foo[]"));
        assert!(key_needs_quoting("foo[0]"));
    }
}
