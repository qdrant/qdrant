use std::borrow::Cow;

use bytemuck::must_cast_slice;
use half::f16;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use zerocopy::IntoBytes;

use super::named_vectors::CowMultiVector;
use super::vectors::TypedMultiDenseVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

pub trait PrimitiveVectorElement:
    Copy + Clone + Default + Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static
{
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>) -> Cow<[Self]>;

    fn slice_to_float_cow(vector: Cow<[Self]>) -> Cow<[VectorElementType]>;

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]>;

    fn datatype() -> VectorStorageDatatype;

    fn from_float_multivector(
        multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self>;

    fn into_float_multivector(
        multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType>;

    /// Cast elements to a byte slice. Typically a wrapper around [`zerocopy`]
    /// or [`bytemuck`] methods.
    ///
    /// TODO: once [`half::f16`] support the latest [`zerocopy`], we could
    /// remove this method in favor of using [`zerocopy::IntoBytes`] directly.
    fn as_bytes(vector: &[Self]) -> &[u8];
}

impl PrimitiveVectorElement for VectorElementType {
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>) -> Cow<[Self]> {
        vector
    }

    fn slice_to_float_cow(vector: Cow<[Self]>) -> Cow<[VectorElementType]> {
        vector
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        _distance: Distance,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]> {
        Cow::Borrowed(vector)
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn from_float_multivector(
        multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self> {
        multivector
    }

    fn into_float_multivector(
        multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType> {
        multivector
    }

    fn as_bytes(vector: &[Self]) -> &[u8] {
        IntoBytes::as_bytes(vector)
    }
}

impl PrimitiveVectorElement for VectorElementTypeHalf {
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| f16::from_f32(x)).collect())
    }

    fn slice_to_float_cow(vector: Cow<[Self]>) -> Cow<[VectorElementType]> {
        Cow::Owned(vector.iter().map(|&x| f16::to_f32(x)).collect_vec())
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        _distance: Distance,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]> {
        Cow::Owned(vector.iter().map(|&x| f16::to_f32(x)).collect_vec())
    }

    fn from_float_multivector(
        multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self> {
        CowMultiVector::Owned(TypedMultiDenseVector::new(
            multivector
                .as_vec_ref()
                .flattened_vectors
                .iter()
                .map(|&x| f16::from_f32(x))
                .collect_vec(),
            multivector.as_vec_ref().dim,
        ))
    }

    fn into_float_multivector(
        multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType> {
        CowMultiVector::Owned(TypedMultiDenseVector::new(
            multivector
                .as_vec_ref()
                .flattened_vectors
                .iter()
                .map(|&x| f16::to_f32(x))
                .collect_vec(),
            multivector.as_vec_ref().dim,
        ))
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Float16
    }

    fn as_bytes(vector: &[Self]) -> &[u8] {
        must_cast_slice(vector)
    }
}

impl PrimitiveVectorElement for VectorElementTypeByte {
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| x as u8).collect())
    }

    fn slice_to_float_cow(vector: Cow<[Self]>) -> Cow<[VectorElementType]> {
        Cow::Owned(
            vector
                .iter()
                .map(|&x| VectorElementType::from(x))
                .collect_vec(),
        )
    }

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]> {
        if let QuantizationConfig::Binary(_) = quantization_config {
            Cow::from(
                vector
                    .iter()
                    .map(|&x| VectorElementType::from(x) - 127.0)
                    .collect_vec(),
            )
        } else {
            let vector = vector
                .iter()
                .map(|&x| VectorElementType::from(x))
                .collect_vec();
            let preprocessed_vector = match distance {
                Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(vector),
                Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(vector),
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementType>>::preprocess(vector)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementType>>::preprocess(vector)
                }
            };
            Cow::from(preprocessed_vector)
        }
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Uint8
    }

    fn from_float_multivector(
        multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self> {
        CowMultiVector::Owned(TypedMultiDenseVector::new(
            multivector
                .as_vec_ref()
                .flattened_vectors
                .iter()
                .map(|&x| x as Self)
                .collect_vec(),
            multivector.as_vec_ref().dim,
        ))
    }

    fn into_float_multivector(
        multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType> {
        CowMultiVector::Owned(TypedMultiDenseVector::new(
            multivector
                .as_vec_ref()
                .flattened_vectors
                .iter()
                .map(|&x| VectorElementType::from(x))
                .collect_vec(),
            multivector.as_vec_ref().dim,
        ))
    }

    fn as_bytes(vector: &[Self]) -> &[u8] {
        IntoBytes::as_bytes(vector)
    }
}
