use std::borrow::Cow;

use bytemuck::{Pod, Zeroable};
use half::f16;
use itertools::Itertools;
use quantization::{DistanceType, EncodedQueryTQ, TQBits, TQMode, TurboQuantizer};
use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::named_vectors::CowMultiVector;
use super::vectors::TypedMultiDenseVector;
use crate::data_types::vectors::{
    TypedDenseVector, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf,
};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

pub trait PrimitiveVectorElement
where
    Self: Copy + Clone + Default + Send + Sync + 'static,
    Self: Serialize + for<'a> Deserialize<'a>,
    Self: FromBytes + Immutable + IntoBytes + KnownLayout,
    Self: Pod,
{
    type QueryType;

    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, distance: Distance) -> Cow<[Self]>;

    fn slice_to_float_cow(
        vector: Cow<[Self]>,
        distance: Distance,
        dim: usize,
    ) -> Cow<[VectorElementType]>;

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        distance: Distance,
    ) -> Self::QueryType;

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
        dim: usize,
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
    type QueryType = TypedDenseVector<Self>;

    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, _distance: Distance) -> Cow<[Self]> {
        vector
    }

    fn slice_to_float_cow(
        vector: Cow<[Self]>,
        _distance: Distance,
        _dim: usize,
    ) -> Cow<[VectorElementType]> {
        vector
    }

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        _distance: Distance,
    ) -> Self::QueryType {
        TypedDenseVector::from(vector)
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        _distance: Distance,
        vector: Cow<'a, [Self]>,
        _dim: usize,
    ) -> Cow<'a, [f32]> {
        vector
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
    type QueryType = TypedDenseVector<Self>;

    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, _distance: Distance) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| f16::from_f32(x)).collect())
    }

    fn slice_to_float_cow(
        vector: Cow<[Self]>,
        _distance: Distance,
        _dim: usize,
    ) -> Cow<[VectorElementType]> {
        Cow::Owned(vector.iter().map(|&x| f16::to_f32(x)).collect_vec())
    }

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        _distance: Distance,
    ) -> Self::QueryType {
        TypedDenseVector::from(vector.iter().map(|&x| f16::from_f32(x)).collect::<Vec<_>>())
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        _distance: Distance,
        vector: Cow<'a, [Self]>,
        _dim: usize,
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
    type QueryType = TypedDenseVector<Self>;

    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, _distance: Distance) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| x as u8).collect())
    }

    fn slice_to_float_cow(
        vector: Cow<[Self]>,
        _distance: Distance,
        _dim: usize,
    ) -> Cow<[VectorElementType]> {
        Cow::Owned(
            vector
                .iter()
                .map(|&x| VectorElementType::from(x))
                .collect_vec(),
        )
    }

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        _distance: Distance,
    ) -> Self::QueryType {
        TypedDenseVector::from(vector.iter().map(|&x| x as u8).collect::<Vec<_>>())
    }

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
        _dim: usize,
    ) -> Cow<'a, [f32]> {
        if let QuantizationConfig::Binary(_) = quantization_config {
            Cow::Owned(
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
            Cow::Owned(distance.preprocess_vector::<VectorElementType>(vector))
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

#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    Serialize,
    Deserialize,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
    Zeroable,
    Pod,
)]
#[repr(transparent)]
pub struct TurboQuantElement(pub u8);

impl PrimitiveVectorElement for TurboQuantElement {
    type QueryType = EncodedQueryTQ;

    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, distance: Distance) -> Cow<[Self]> {
        let api_dim = vector.len();
        let quantizer = TurboQuantizer::new_fast_forward(
            api_dim,
            TQBits::Bits4,
            TQMode::Normal,
            DistanceType::from(distance),
            None,
        );
        let mut buf = vec![0.0_f64; quantizer.quantize_buffer_len()];
        let bytes = quantizer.quantize(&vector, &mut buf);
        Cow::Owned(bytemuck::allocation::cast_vec(bytes))
    }

    fn slice_to_float_cow(
        vector: Cow<[Self]>,
        distance: Distance,
        dim: usize,
    ) -> Cow<[VectorElementType]> {
        let quantizer = TurboQuantizer::new(
            dim,
            TQBits::Bits4,
            TQMode::Normal,
            DistanceType::from(distance),
            None,
        );
        let slice: &[Self] = &vector;
        let mut deq = quantizer.dequantize(bytemuck::cast_slice(slice));
        quantizer.apply_inverse_rotation(&mut deq);
        deq.truncate(dim);
        Cow::Owned(deq.into_iter().map(|x| x as f32).collect())
    }

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        distance: Distance,
    ) -> Self::QueryType {
        let api_dim = vector.len();
        let quantizer = TurboQuantizer::new_fast_forward(
            api_dim,
            TQBits::Bits4,
            TQMode::Normal,
            DistanceType::from(distance),
            None,
        );
        quantizer.precompute_query(&vector)
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
        dim: usize,
    ) -> Cow<'a, [f32]> {
        let quantizer = TurboQuantizer::new(
            dim,
            TQBits::Bits4,
            TQMode::Normal,
            DistanceType::from(distance),
            None,
        );
        let slice: &[Self] = &vector;
        let mut deq = quantizer.dequantize(bytemuck::cast_slice(slice));
        if distance == Distance::Manhattan {
            quantizer.apply_inverse_rotation(&mut deq);
        }
        Cow::Owned(deq.into_iter().map(|x| x as f32).collect())
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Turbo4
    }

    fn from_float_multivector(
        _multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self> {
        unimplemented!("TurboQuantElement::from_float_multivector")
    }

    fn into_float_multivector(
        _multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType> {
        unimplemented!("TurboQuantElement::into_float_multivector")
    }
}
