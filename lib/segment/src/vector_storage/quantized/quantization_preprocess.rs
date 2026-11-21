use std::borrow::Cow;
use std::mem::size_of;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

/// Result of decoding a storage-native row for quantization.
///
/// Turbo datatypes keep their existing requantization path. In particular, `Turbo4` is an
/// intentional exclusion from the primitive typed preprocessing fixed here.
pub(super) enum RawPreprocess<'a> {
    Preprocessed(Cow<'a, [VectorElementType]>),
    Untouched,
}

fn preprocess_typed<'a, T: PrimitiveVectorElement>(
    quantization_config: &QuantizationConfig,
    distance: Distance,
    vector: Cow<'a, [T]>,
) -> Cow<'a, [VectorElementType]> {
    T::quantization_preprocess(quantization_config, distance, vector)
}

fn preprocess_decoded_as<'a, T: PrimitiveVectorElement>(
    quantization_config: &QuantizationConfig,
    distance: Distance,
    vector: &'a [VectorElementType],
) -> Cow<'a, [VectorElementType]> {
    preprocess_typed(
        quantization_config,
        distance,
        T::slice_from_float_cow(Cow::Borrowed(vector)),
    )
}

/// Convert an incoming decoded vector through its configured raw primitive, then apply the same
/// quantization preprocessing used by `QuantizedVectors::create_impl`.
pub(super) fn preprocess_decoded<'a>(
    datatype: VectorStorageDatatype,
    quantization_config: &QuantizationConfig,
    distance: Distance,
    vector: &'a [VectorElementType],
) -> Cow<'a, [VectorElementType]> {
    match datatype {
        VectorStorageDatatype::Float32 => {
            preprocess_decoded_as::<VectorElementType>(quantization_config, distance, vector)
        }
        VectorStorageDatatype::Float16 => {
            preprocess_decoded_as::<VectorElementTypeHalf>(quantization_config, distance, vector)
        }
        VectorStorageDatatype::Uint8 => {
            preprocess_decoded_as::<VectorElementTypeByte>(quantization_config, distance, vector)
        }
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => Cow::Borrowed(vector),
    }
}

/// Preprocess every inner vector independently, preserving the flattened order and count.
pub(super) fn preprocess_decoded_multi<'a>(
    datatype: VectorStorageDatatype,
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    flattened: &'a [VectorElementType],
) -> Cow<'a, [VectorElementType]> {
    debug_assert!(dim > 0 && flattened.len().is_multiple_of(dim));
    match datatype {
        VectorStorageDatatype::Float32
        | VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => {
            preprocess_decoded(datatype, quantization_config, distance, flattened)
        }
        VectorStorageDatatype::Float16 | VectorStorageDatatype::Uint8 => Cow::Owned(
            flattened
                .chunks_exact(dim)
                .flat_map(|inner| {
                    preprocess_decoded(datatype, quantization_config, distance, inner).into_owned()
                })
                .collect(),
        ),
    }
}

fn preprocess_raw_as<'a, T: PrimitiveVectorElement>(
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    bytes: &'a [u8],
) -> OperationResult<Cow<'a, [VectorElementType]>> {
    let expected_size = dim * size_of::<T>();
    if bytes.len() != expected_size {
        return Err(OperationError::malformed_vector_blob(format!(
            "Malformed dense vector blob of {} bytes, expected {expected_size}",
            bytes.len(),
        )));
    }

    // A misaligned WAL blob decodes through a copy, matching the previous update-only path.
    let vector: Cow<'a, [T]> = match bytemuck::try_cast_slice(bytes) {
        Ok(slice) => Cow::Borrowed(slice),
        Err(_) => Cow::Owned(bytemuck::allocation::pod_collect_to_vec(bytes)),
    };
    Ok(preprocess_typed(quantization_config, distance, vector))
}

/// Decode and preprocess a storage-native primitive row. Turbo rows are returned explicitly as
/// untouched so their established quantizer-specific dequantization route remains in control.
pub(super) fn preprocess_raw<'a>(
    datatype: VectorStorageDatatype,
    quantization_config: &QuantizationConfig,
    distance: Distance,
    dim: usize,
    bytes: &'a [u8],
) -> OperationResult<RawPreprocess<'a>> {
    let vector = match datatype {
        VectorStorageDatatype::Float32 => {
            preprocess_raw_as::<VectorElementType>(quantization_config, distance, dim, bytes)?
        }
        VectorStorageDatatype::Float16 => {
            preprocess_raw_as::<VectorElementTypeHalf>(quantization_config, distance, dim, bytes)?
        }
        VectorStorageDatatype::Uint8 => {
            preprocess_raw_as::<VectorElementTypeByte>(quantization_config, distance, dim, bytes)?
        }
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => return Ok(RawPreprocess::Untouched),
    };
    Ok(RawPreprocess::Preprocessed(vector))
}
