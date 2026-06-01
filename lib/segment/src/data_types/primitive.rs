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

    fn slice_to_float_cow(vector: Cow<[Self]>, distance: Distance) -> Cow<[VectorElementType]>;

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        distance: Distance,
    ) -> Self::QueryType;

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
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

    fn slice_to_float_cow(vector: Cow<[Self]>, _distance: Distance) -> Cow<[VectorElementType]> {
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

    fn slice_to_float_cow(vector: Cow<[Self]>, _distance: Distance) -> Cow<[VectorElementType]> {
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

    fn slice_to_float_cow(vector: Cow<[Self]>, _distance: Distance) -> Cow<[VectorElementType]> {
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

const TQ_BITS: TQBits = TQBits::Bits4;
const TQ_MODE: TQMode = TQMode::Normal;

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
        let quantizer = TurboQuantizer::new(
            api_dim,
            TQ_BITS,
            TQ_MODE,
            DistanceType::from(distance),
            None,
        );
        let mut buf = vec![0.0_f64; quantizer.quantize_buffer_len()];
        let bytes = quantizer.quantize(&vector, &mut buf);
        Cow::Owned(bytemuck::allocation::cast_vec(bytes))
    }

    fn slice_to_float_cow(vector: Cow<[Self]>, distance: Distance) -> Cow<[VectorElementType]> {
        decode_tq(&vector, distance)
    }

    fn query_from_float_cow(
        vector: Cow<[VectorElementType]>,
        distance: Distance,
    ) -> Self::QueryType {
        let api_dim = vector.len();
        let quantizer = TurboQuantizer::new(
            api_dim,
            TQ_BITS,
            TQ_MODE,
            DistanceType::from(distance),
            None,
        );
        quantizer.precompute_query(&vector)
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
    ) -> Cow<'a, [f32]> {
        decode_tq(&vector, distance)
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Uint8
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

pub(crate) fn quantizer_for_tq_slot(slot_len: usize, distance: Distance) -> TurboQuantizer {
    let tq_distance = DistanceType::from(distance);
    let extras_size = TurboQuantizer::quantized_size_for(0, TQ_BITS, tq_distance, TQ_MODE);
    let packed_bytes = slot_len
        .checked_sub(extras_size)
        .expect("slot shorter than TurboQuant extras trailer");
    let padded_dim = padded_bytes_to_dim(packed_bytes);
    TurboQuantizer::new(padded_dim, TQ_BITS, TQ_MODE, tq_distance, None)
}

fn decode_tq(
    vector: &[TurboQuantElement],
    distance: Distance,
) -> Cow<'static, [VectorElementType]> {
    let quantizer = quantizer_for_tq_slot(vector.len(), distance);
    let bytes: &[u8] = bytemuck::cast_slice(vector);
    let mut deq = quantizer.dequantize(bytes);
    quantizer.apply_inverse_rotation(&mut deq);
    Cow::Owned(deq.into_iter().map(|x| x as f32).collect())
}

fn padded_bytes_to_dim(packed_bytes: usize) -> usize {
    let extras = TurboQuantizer::quantized_size_for(0, TQ_BITS, DistanceType::Cosine, TQ_MODE);
    let probe_dim = 64;
    let probe_packed =
        TurboQuantizer::quantized_size_for(probe_dim, TQ_BITS, DistanceType::Cosine, TQ_MODE)
            - extras;
    packed_bytes * probe_dim / probe_packed
}
