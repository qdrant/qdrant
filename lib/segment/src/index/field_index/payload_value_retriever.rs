//! Fallible point-to-values reads for internal payload projections.

use std::cell::RefCell;

use common::counter::hardware_counter::HardwareCounterCell;
use serde_json::Value;

use super::map_index::read_ops::MapIndexRead;
use crate::common::operation_error::OperationResult;
use crate::common::utils::MultiValue;
use crate::index::field_index::PayloadValueRetriever;
use crate::types::IntPayloadType;

/// Visit every value through the fallible checker. The legacy `get_values`
/// accessors can turn an underlying I/O error into a missing value.
pub(crate) fn collect<T: ?Sized>(
    read: impl FnOnce(&dyn Fn(&T) -> bool) -> OperationResult<bool>,
    convert: impl Fn(&T) -> Option<Value>,
) -> OperationResult<MultiValue<Value>> {
    let values = RefCell::new(MultiValue::new());
    read(&|value| {
        if let Some(value) = convert(value) {
            values.borrow_mut().push(value);
        }
        false
    })?;
    Ok(values.into_inner())
}

pub(crate) fn keyword<'a, T: MapIndexRead<'a, str> + 'a>(
    index: &'a T,
    hw_counter: &'a HardwareCounterCell,
) -> PayloadValueRetriever<'a> {
    Box::new(move |point_id| {
        collect(
            |visit| index.check_values_any(point_id, hw_counter, visit),
            |value| Some(Value::String(value.to_owned())),
        )
    })
}

pub(crate) fn integer<'a, T: MapIndexRead<'a, IntPayloadType> + 'a>(
    index: &'a T,
    hw_counter: &'a HardwareCounterCell,
) -> PayloadValueRetriever<'a> {
    Box::new(move |point_id| {
        collect(
            |visit| index.check_values_any(point_id, hw_counter, visit),
            |value| Some(Value::from(*value)),
        )
    })
}
