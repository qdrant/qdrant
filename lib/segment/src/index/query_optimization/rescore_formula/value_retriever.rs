use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::utils::MultiValue;

pub type VariableRetrieverFn<'a> = Box<dyn Fn(PointOffsetType) -> MultiValue<Value> + 'a>;
