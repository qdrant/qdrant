use crate::{FixedSizedValue, VarSizedValue};

/// Trait to abstract the handling of values in PostingList
///
/// This trait handles the differences between fixed-size and variable-size value
/// implementations, allowing us to have a unified implementation of `from_builder`.
///
/// - For fixed-size values, the storage type S is the same as the value type V
/// - For variable-size values, S is a pointer/offset (u32) into the var_sized_data
pub(crate) trait ValueHandler<V, S: Sized + Copy> {
    /// Process values before storage and return the necessary var_sized_data
    ///
    /// For fixed-size values, this returns the values themselves and an empty var_sized_data
    /// For variable-size values, this returns offsets and the actual serialized data
    fn process_values(values: Vec<V>) -> (Vec<S>, Vec<u8>);
}

/// Handler for fixed-sized values
pub(crate) struct FixedSizeHandler;

/// Handler for variable-sized values
pub(crate) struct VarSizeHandler;

impl<V: FixedSizedValue + Copy + Default> ValueHandler<V, V> for FixedSizeHandler {
    fn process_values(values: Vec<V>) -> (Vec<V>, Vec<u8>) {
        (values, Vec::new())
    }
}

impl<V: VarSizedValue + Clone> ValueHandler<V, u32> for VarSizeHandler {
    fn process_values(values: Vec<V>) -> (Vec<u32>, Vec<u8>) {
        let mut var_sized_data = Vec::new();
        let mut offsets = Vec::with_capacity(values.len());
        let mut current_offset = 0u32;

        for value in values {
            offsets.push(current_offset);
            let bytes = value.to_bytes();
            current_offset += bytes.len() as u32;
            var_sized_data.extend_from_slice(&bytes);
        }

        (offsets, var_sized_data)
    }
}
