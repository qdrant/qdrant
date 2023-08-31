
use segment::types::{ScoredPoint, PayloadKeyType};
use serde_json::Value;

pub enum Operation {
    Sum,
    Min,
    Max,
    Mean
}

pub struct AggregateFn {
    pub name: String,
    pub op: Operation,
    pub arg_keys: Vec<PayloadKeyType>,
    pub strict: bool
}

impl AggregateFn {

    pub fn extract_params(_s: &str) -> Vec<PayloadKeyType> {
        return vec!["foo".to_string()];
    }

    pub fn parse(s: &str) -> Option<Self> {
        if s == "sum(foo)" {
            Some(Self {
                name: s.to_string(),
                op: Operation::Sum,
                arg_keys: vec!["foo".to_string()],
                strict: false
            })
        } else {
            None
        }
    }

    pub fn run(&self, _points: &[ScoredPoint]) -> Value {
        // TODO: Short-circuit on empty
        todo!();

        // TODO: No unwrap
        //Value::Number(Number::from_f64(0.0).unwrap())
    }
}