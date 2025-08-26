use std::borrow::Cow;

use half::f16;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::named_vectors::CowMultiVector;
use super::vectors::TypedMultiDenseVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

pub trait PrimitiveVectorElement
where
    Self: Copy + Clone + Default + Send + Sync + 'static,
    Self: Serialize + for<'a> Deserialize<'a>,
    Self: FromBytes + Immutable + IntoBytes + KnownLayout,
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
            Cow::from(distance.preprocess_vector::<VectorElementType>(vector))
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
}
