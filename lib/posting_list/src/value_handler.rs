use crate::{SizedValue, VarSizedValue};
use std::marker::PhantomData;

/// Marker structs for distinguishing implementations
pub struct SizedMarker;
pub struct VarSizedMarker;

/// Trait to abstract the handling of values in PostingList
///
/// This trait handles the differences between fixed-size and variable-size value
/// implementations, allowing us to have a unified implementation of `from_builder`.
///
/// - For fixed-size values, the associated type [`ValueHandler::Fixed`] is the same as the generic type V
/// - For variable-size values, [`ValueHandler::Fixed`] is an offset (u32) into the var_sized_data
pub trait ValueHandler<V> {
    /// The value to store within each chunk, or alongside each id.
    type Sized: std::marker::Sized + Copy;

    /// Process values before storage and return the necessary var_sized_data
    ///
    /// For fixed-size values, this returns the values themselves and an empty var_sized_data
    /// For variable-size values, this returns offsets and the actual serialized data
    fn process_values(values: Vec<V>) -> (Vec<Self::Sized>, Vec<u8>);

    /// Retrieve a value.
    ///
    /// For sized values it returns the first argument.
    /// For variable-size values it returns the value between the two sized values in var_data.
    fn get_value(
        sized_value: Self::Sized,
        next_sized_value: Option<Self::Sized>,
        var_data: &[u8],
    ) -> V;
}

/// Fixed-size value handler
pub struct Sized<V>(PhantomData<V>);

impl<V: SizedValue + Copy> ValueHandler<V> for Sized<V> {
    type Sized = V;

    fn process_values(values: Vec<V>) -> (Vec<V>, Vec<u8>) {
        (values, Vec::new())
    }

    fn get_value(sized_value: V, _next_sized_value: Option<V>, _var_data: &[u8]) -> V {
        sized_value
    }
}

/// Var-size value handler
pub struct VarSized<V>(PhantomData<V>);

impl<V: VarSizedValue> ValueHandler<V> for VarSized<V> {
    type Sized = u32;

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

    fn get_value(sized_value: u32, next_sized_value: Option<u32>, var_data: &[u8]) -> V {
        let range = match next_sized_value {
            Some(next_value) => sized_value as usize..next_value as usize,
            None => sized_value as usize..var_data.len(),
        };
        V::from_bytes(&var_data[range])
    }
}
