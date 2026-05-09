use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;

pub trait ValueIndexer {
    type ValueType;

    /// Add multiple values associated with a single point
    /// This function should be called only once for each point
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Extract index-able value from payload `Value`
    fn get_value(value: &Value) -> Option<Self::ValueType>;

    /// Try to extract index-able values from payload `Value`, even if it is an array
    fn get_values(value: &Value) -> Vec<Self::ValueType> {
        match value {
            Value::Array(values) => values.iter().filter_map(|x| Self::get_value(x)).collect(),
            _ => Self::get_value(value).map(|x| vec![x]).unwrap_or_default(),
        }
    }

    /// Add point with payload to index
    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.remove_point(id)?;
        let mut flatten_values: Vec<_> = vec![];
        for value in payload {
            match value {
                Value::Array(values) => {
                    flatten_values.extend(values.iter().filter_map(|x| Self::get_value(x)));
                }
                _ => {
                    if let Some(x) = Self::get_value(value) {
                        flatten_values.push(x);
                    }
                }
            }
        }
        self.add_many(id, flatten_values, hw_counter)
    }

    /// remove a point from the index
    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()>;
}
