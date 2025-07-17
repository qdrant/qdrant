use std::fmt::Debug;
use std::marker::PhantomData;

use zerocopy::little_endian::U32;

use crate::{SizedValue, UnsizedValue};

/// Trait for types that can be used as posting list values
///
/// This trait associates a value type with its appropriate handler.
pub trait PostingValue: Clone {
    type Handler: ValueHandler<Value = Self>;
}

// Implementations for built-in sized types
impl PostingValue for () {
    type Handler = SizedHandler<()>;
}
impl PostingValue for u32 {
    type Handler = SizedHandler<u32>;
}
impl PostingValue for u64 {
    type Handler = SizedHandler<u64>;
}

/// Trait to abstract the handling of values in PostingList
///
/// This trait handles the differences between fixed-size and variable-size value
/// implementations, allowing us to have a unified implementation of `from_builder`.
///
/// - For fixed-size values, the associated type [`ValueHandler::Sized`] is the same as the generic type V
/// - For variable-size values, [`ValueHandler::Sized`] is an offset into the var_sized_data
pub trait ValueHandler {
    /// The type of value in each PostingElement.
    type Value;
    /// The value to store within each chunk, or alongside each id.
    type Sized: std::marker::Sized + Copy + Debug;

    /// Process values before storage and return the necessary var_sized_data
    ///
    /// - For fixed-size values, this returns the values themselves and an empty var_sized_data.
    /// - For variable-size values, this returns offsets and the flattened serialized data.
    fn process_values(values: Vec<Self::Value>) -> (Vec<Self::Sized>, Vec<u8>);

    /// Retrieve a value.
    ///
    /// - For sized values it returns the first argument.
    /// - For variable-size values it returns the value between the two sized values in var_data.
    fn get_value<N>(sized_value: Self::Sized, next_sized_value: N, var_data: &[u8]) -> Self::Value
    where
        N: Fn() -> Option<Self::Sized>;
}

/// Fixed-size value handler
#[derive(Default, Debug, Clone, Copy)]
pub struct SizedHandler<V: SizedValue>(PhantomData<V>);

impl<V: SizedValue> ValueHandler for SizedHandler<V> {
    type Value = V;
    type Sized = V;

    fn process_values(values: Vec<V>) -> (Vec<V>, Vec<u8>) {
        (values, Vec::new())
    }

    fn get_value<N>(sized_value: V, _next_sized_value: N, _var_data: &[u8]) -> V
    where
        N: Fn() -> Option<Self::Sized>,
    {
        sized_value
    }
}

/// Var-size value handler
#[derive(Default, Debug, Clone, Copy)]
pub struct UnsizedHandler<V: UnsizedValue>(PhantomData<V>);

impl<V: UnsizedValue> ValueHandler for UnsizedHandler<V> {
    type Value = V;
    type Sized = U32;

    fn process_values(values: Vec<Self::Value>) -> (Vec<Self::Sized>, Vec<u8>) {
        let mut offsets = Vec::with_capacity(values.len());
        let mut current_offset = 0u32;

        for value in &values {
            offsets.push(U32::from(current_offset));
            let value_len = u32::try_from(value.write_len())
                .expect("Value larger than 4GB, use u64 offsets instead");
            // prepare next starting offset
            current_offset = current_offset
                .checked_add(value_len)
                .expect("Size of all values exceeds 4GB");
        }

        let last_offset = offsets.last();
        let ranges = offsets
            .windows(2)
            .map(|w| w[0].get() as usize..w[1].get() as usize)
            // the last one is not included in windows, but goes until the end
            .chain(last_offset.map(|&last| last.get() as usize..current_offset as usize));

        let mut var_sized_data = vec![0; current_offset as usize];
        for (value, range) in values.iter().zip(ranges) {
            value.write_to(&mut var_sized_data[range]);
        }

        (offsets, var_sized_data)
    }

    fn get_value<N>(sized_value: Self::Sized, next_sized_value: N, var_data: &[u8]) -> Self::Value
    where
        N: Fn() -> Option<Self::Sized>,
    {
        let range = match next_sized_value() {
            Some(next_value) => sized_value.get() as usize..next_value.get() as usize,
            None => sized_value.get() as usize..var_data.len(),
        };

        V::from_bytes(&var_data[range])
    }
}
