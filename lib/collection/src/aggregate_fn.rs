use nom::{IResult, bytes::complete::{tag, take_while1}, branch::alt, combinator::{value, opt}, Parser, sequence::{terminated, delimited, tuple}, character::complete::multispace0};
use std::str::FromStr;

use segment::types::{ScoredPoint, PayloadKeyType};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Err {
    ParsingError
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Sum,
    Min,
    Max,
    Mean
}

impl FromStr for Operation {
    type Err = Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::*;

        match s {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "mean" => Ok(Self::Mean),
            _ => Err(Err::ParsingError)
        }
    }
}

fn parse_ident(i: &str) -> IResult<&str, &str> {
    // TODO: Parse all valid JSON keys and key-paths
    take_while1(move |c: char| c.is_alphanumeric())(i)
}

fn parse_op(i: &str) -> IResult<&str, Operation> {
    alt((
        value(Operation::Sum, tag("sum")),
        value(Operation::Min, tag("min")),
        value(Operation::Max, tag("max")),
        value(Operation::Mean, tag("mean")),
    )).parse(i)
}

fn parse_arglist(i: &str) -> IResult<&str, Vec<PayloadKeyType>> {
    delimited(
        tag("("),
        parse_ident,
        tag(")"))
    .map(|s| vec![s.to_string()])
    .parse(i)
}

fn parse_strict(i: &str) -> IResult<&str, bool> {
    opt(terminated(tag("strict:"), multispace0))
        .map(|o| o.is_some())
        .parse(i)
}

/// Grammar:
/// 
/// function -> function_body | strict: function_body
/// 
/// function_body -> operation(parameter)
/// operation -> sum | min | max | mean 
/// parameter -> string
/// 
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateFn {
    pub name: String,
    pub op: Operation,
    pub arg_keys: Vec<PayloadKeyType>,
    pub strict: bool
}

impl AggregateFn {

    pub fn extract_params(s: &str) -> Result<Vec<PayloadKeyType>, Err> {
        let func = Self::parse(s)?;
        Ok(func.arg_keys)
    }

    pub fn parse(mut input: &str) -> Result<Self, Err> {
        input = input.trim();

        let (strict, op, arg_keys) = 
        tuple((
            parse_strict,
            parse_op,
            parse_arglist
        ))
            .parse(input)
            .map_err(|_| Err::ParsingError)?
            .1;

        Ok(Self {
            name: input.to_string(),
            strict,
            op,
            arg_keys
        })
    }

    pub fn run(&self, _points: &[ScoredPoint]) -> Value {
        // TODO: Short-circuit on empty
        todo!();

        // TODO: No unwrap
        //Value::Number(Number::from_f64(0.0).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;
    use super::AggregateFn;

    #[test]
    fn test_parse_aggr_fn() {
        assert_eq!(AggregateFn::parse("sum(foo)"), Ok(AggregateFn {
            name: "sum(foo)".to_string(),
            op: Operation::Sum,
            arg_keys: vec!["foo".to_string()],
            strict: false
        }));

        assert_eq!(AggregateFn::parse("  strict: mean(bar)  "), Ok(AggregateFn {
            name: "strict: mean(bar)".to_string(),
            op: Operation::Mean,
            arg_keys: vec!["bar".to_string()],
            strict: true
        }));

        assert!(AggregateFn::parse("???").is_err());
        assert!(AggregateFn::parse("sum(sum(foo))").is_err());
        assert!(AggregateFn::parse("sum()").is_err());
        assert!(AggregateFn::parse("whatever(foo)").is_err());
    }
}