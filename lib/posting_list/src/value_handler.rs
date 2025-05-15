use std::marker::PhantomData;

use crate::{SizedValue, VarSizedValue};

/// Trait to abstract the handling of values in PostingList
///
/// This trait handles the differences between fixed-size and variable-size value
/// implementations, allowing us to have a unified implementation of `from_builder`.
///
/// - For fixed-size values, the associated type [`ValueHandler::Sized`] is the same as the generic type V
/// - For variable-size values, [`ValueHandler::Sized`] is an offset into the var_sized_data
pub trait ValueHandler {
    /// The type of value in each PostingElement.
    type Value: std::fmt::Debug;
    /// The value to store within each chunk, or alongside each id.
    type Sized: std::fmt::Debug + std::marker::Sized + Copy;

    /// Process values before storage and return the necessary var_sized_data
    ///
    /// - For fixed-size values, this returns the values themselves and an empty var_sized_data.
    /// - For variable-size values, this returns offsets and the flattened serialized data.
    fn process_values(values: Vec<Self::Value>) -> (Vec<Self::Sized>, Vec<u8>);

    /// Retrieve a value.
    ///
    /// - For sized values it returns the first argument.
    /// - For variable-size values it returns the value between the two sized values in var_data.
    fn get_value<N>(
        sized_value: Self::Sized,
        next_sized_value: N,
        var_data: &[u8],
    ) -> Self::Value
    where N: Fn() -> Option<Self::Sized>;
}

/// Fixed-size value handler
pub struct Sized<V>(PhantomData<V>);

impl<V: SizedValue + Copy> ValueHandler for Sized<V> {
    type Value = V;
    type Sized = V;

    fn process_values(values: Vec<V>) -> (Vec<V>, Vec<u8>) {
        (values, Vec::new())
    }

    fn get_value<N>(sized_value: V, _next_sized_value: N, _var_data: &[u8]) -> V where N: Fn() -> Option<Self::Sized> {
        sized_value
    }
}

/// Var-size value handler
pub struct VarSized<V>(PhantomData<V>);

impl<V: VarSizedValue> ValueHandler for VarSized<V> {
    type Value = V;
    type Sized = u32;

    fn process_values(values: Vec<Self::Value>) -> (Vec<Self::Sized>, Vec<u8>) {
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

    fn get_value<N>(
        sized_value: Self::Sized,
        next_sized_value: N,
        var_data: &[u8],
    ) -> Self::Value where N: Fn() -> Option<Self::Sized> {
        let range = match next_sized_value() {
            Some(next_value) => sized_value as usize..next_value as usize,
            None => sized_value as usize..var_data.len(),
        };
        V::from_bytes(&var_data[range])
    }
}
