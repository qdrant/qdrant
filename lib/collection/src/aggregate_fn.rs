use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::multispace0;
use nom::combinator::{opt, value};
use nom::sequence::{delimited, terminated, tuple};
use nom::{IResult, Parser};
use segment::types::{PayloadKeyType, ScoredPoint};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Err {
    ParsingError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Sum,
    Min,
    Max,
    Mean,
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
            _ => Err(Err::ParsingError),
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
    ))
    .parse(i)
}

fn parse_arglist(i: &str) -> IResult<&str, Vec<PayloadKeyType>> {
    delimited(tag("("), parse_ident, tag(")"))
        .map(|s| vec![s.to_string()])
        .parse(i)
}

fn parse_strict(i: &str) -> IResult<&str, bool> {
    opt(terminated(tag("strict:"), multispace0))
        .map(|o| o.is_some())
        .parse(i)
}

fn get_numeric_argument(point: &ScoredPoint, at: usize) -> Option<f64> {
    let arglist = point.aggregate_args.as_ref()?;
    let arg = arglist.get(at)?;
    // TODO: Should this work with longer lists?
    if arg.len() != 1 {
        return None;
    };

    arg[0].as_f64()
}

fn do_sum(points: &[ScoredPoint], strict: bool, arg_idx: usize) -> Option<f64> {
    // TODO: numerically stable summation
    let mut sum = 0.0f64;

    for p in points {
        let arg = get_numeric_argument(p, arg_idx);
        if strict && arg.is_none() {
            return None;
        }
        let arg = arg.unwrap_or_default();
        sum += arg;
    }

    Some(sum)
}

fn do_minmax(points: &[ScoredPoint], strict: bool, arg_idx: usize, max: bool) -> Option<f64> {
    let mut res = points.first().map(|p| get_numeric_argument(p, arg_idx))??;
    let cmp = if max { f64::max } else { f64::min };

    for p in points {
        let arg = get_numeric_argument(p, arg_idx);
        if strict && arg.is_none() {
            return None;
        }
        let arg = arg.unwrap_or_default();
        res = cmp(res, arg);
    }

    Some(res)
}

fn do_mean(points: &[ScoredPoint], strict: bool, arg_idx: usize) -> Option<f64> {
    if points.len() == 0 {
        return None;
    };
    let sum = do_sum(points, strict, arg_idx)?;
    Some(sum / points.len() as f64)
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
    pub strict: bool,
}

impl AggregateFn {
    pub fn extract_params(s: &str) -> Result<Vec<PayloadKeyType>, Err> {
        let func = Self::parse(s)?;
        Ok(func.arg_keys)
    }

    pub fn parse(mut input: &str) -> Result<Self, Err> {
        input = input.trim();

        let (strict, op, arg_keys) = tuple((parse_strict, parse_op, parse_arglist))
            .parse(input)
            .map_err(|_| Err::ParsingError)?
            .1;

        Ok(Self {
            name: input.to_string(),
            strict,
            op,
            arg_keys,
        })
    }

    pub fn run(&self, points: &[ScoredPoint]) -> Value {
        if points.is_empty() {
            return Value::Null;
        }

        let res = match self.op {
            Operation::Sum => do_sum(points, self.strict, 0),
            Operation::Min => do_minmax(points, self.strict, 0, false),
            Operation::Max => do_minmax(points, self.strict, 0, true),
            Operation::Mean => do_mean(points, self.strict, 0),
        };

        res.map_or(Value::Null, |res| Value::from(res))
    }
}

#[cfg(test)]
mod tests {
    use segment::types::ScoredPoint;

    use super::{AggregateFn, Operation};

    fn make_points(count: usize) -> Vec<ScoredPoint> {
        (0..count)
            .map(|i| {
                let v = serde_json::Value::from(i);

                ScoredPoint {
                    id: (i as u64).into(),
                    payload: None,
                    score: 0.0,
                    version: 0,
                    vector: None,
                    aggregate_args: Some(vec![vec![v]]),
                }
            })
            .collect()
    }

    #[test]
    fn test_parse_aggr_fn() {
        assert_eq!(
            AggregateFn::parse("sum(foo)"),
            Ok(AggregateFn {
                name: "sum(foo)".to_string(),
                op: Operation::Sum,
                arg_keys: vec!["foo".to_string()],
                strict: false
            })
        );

        assert_eq!(
            AggregateFn::parse("  strict: mean(bar)  "),
            Ok(AggregateFn {
                name: "strict: mean(bar)".to_string(),
                op: Operation::Mean,
                arg_keys: vec!["bar".to_string()],
                strict: true
            })
        );

        assert!(AggregateFn::parse("???").is_err());
        assert!(AggregateFn::parse("sum(sum(foo))").is_err());
        assert!(AggregateFn::parse("sum()").is_err());
        assert!(AggregateFn::parse("whatever(foo)").is_err());
    }

    #[test]
    fn test_simple_fns() {
        let count = 10;
        let points = make_points(count);

        assert_eq!(
            AggregateFn::parse("sum(foo)").unwrap().run(&points),
            serde_json::Value::from((0..count).sum::<usize>() as f64)
        );

        assert_eq!(
            AggregateFn::parse("min(foo)").unwrap().run(&points),
            serde_json::Value::from(0 as f64)
        );

        assert_eq!(
            AggregateFn::parse("max(foo)").unwrap().run(&points),
            serde_json::Value::from((count - 1) as f64)
        );

        assert_eq!(
            AggregateFn::parse("mean(foo)").unwrap().run(&points),
            serde_json::Value::from((0..count).sum::<usize>() as f64 / count as f64)
        );
    }
}
