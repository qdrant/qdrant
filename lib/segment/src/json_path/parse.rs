use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, digit1, none_of, satisfy};
use nom::combinator::{map_res, recognize};
use nom::multi::many0;
use nom::sequence::{delimited, preceded};
use nom::{IResult, Parser};

use super::{JsonPath, JsonPathItem};

#[derive(Debug, PartialEq)]
pub struct ParseError;

impl FromStr for JsonPath {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match json_path(s) {
            Ok(("", path)) => Ok(path),
            _ => Err(ParseError),
        }
    }
}

pub fn key_needs_quoting(s: &str) -> bool {
    raw_str(s).is_err()
}

fn json_path(input: &str) -> IResult<&str, JsonPath> {
    let (input, first_key) = alt((raw_str.map(str::to_string), quoted_str)).parse(input)?;

    let (input, rest) = many0(alt((
        (preceded(char('.'), raw_str).map(|s| JsonPathItem::Key(s.to_string()))),
        (preceded(char('.'), quoted_str).map(JsonPathItem::Key)),
        (delimited(char('['), number, char(']')).map(JsonPathItem::Index)),
        (tag("[]").map(|_| JsonPathItem::WildcardIndex)),
    )))(input)?;

    Ok((input, JsonPath { first_key, rest }))
}

fn raw_str(input: &str) -> IResult<&str, &str> {
    recognize(preceded(
        satisfy(|c: char| c.is_alphabetic() || c == '_' || c == '-'),
        many0(satisfy(|c: char| c.is_alphanumeric() || c == '_' || c == '-').map(|_: char| ())),
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
        assert!("".parse::<JsonPath>().is_err());

        assert_eq!(
            "foo".parse(),
            Ok(JsonPath {
                first_key: "foo".to_string(),
                rest: vec![],
            })
        );

        assert_eq!(
            "foo[1][50].bar-baz[].\"qux[.]quux\"".parse(),
            Ok(JsonPath {
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
}
