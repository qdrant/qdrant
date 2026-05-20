use std::alloc::Layout;
use std::borrow::Cow;

use bytemuck::Pod;
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
    Self: Pod,
{
    /// Encode an api-level float vector into a single on-storage slot.
    ///
    /// Input length is the api-level dimension `api_dim`. Output length is
    /// `Self::storage_len_in_elements(api_dim, distance)` — for flat `T` they
    /// coincide, for `T` carrying metric-dependent side payload the output is
    /// longer and the payload elements live past the api-dim prefix.
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, distance: Distance) -> Cow<[Self]>;

    /// Decode a single on-storage slot back into the api-level float vector.
    ///
    /// Inverse of `slice_from_float_cow`: input length is
    /// `Self::storage_len_in_elements(api_dim, distance)`, output length is
    /// `api_dim`.
    fn slice_to_float_cow(vector: Cow<[Self]>, distance: Distance) -> Cow<[VectorElementType]>;

    /// Decode a slot for use as input to a downstream quantizer.
    ///
    /// Default delegates to `slice_to_float_cow`. Types that store in a
    /// distance-invariant rotated basis (e.g. TurboQuant) skip the inverse
    /// rotation here when the metric tolerates it (L2/Cosine/Dot), so the
    /// downstream quantizer can operate in the same rotated basis and skip
    /// its own rotation step. L1 is rotation-sensitive — those types still
    /// revert to the original basis when `distance == Manhattan`.
    fn decode_for_quantization(
        vector: Cow<[Self]>,
        distance: Distance,
    ) -> Cow<[VectorElementType]> {
        Self::slice_to_float_cow(vector, distance)
    }

    /// Whether [`Self::decode_for_quantization`] returns vectors in a rotated
    /// basis for this metric. Downstream quantizers that have their own
    /// rotation step (TurboQuant) should query this to decide whether to
    /// skip it.
    fn is_prerotated_for_quantization(distance: Distance) -> bool {
        let _ = distance;
        false
    }

    /// Preprocess a single on-storage slot into the api-level float vector
    /// expected by downstream quantization.
    ///
    /// Same length contract as `slice_to_float_cow`: input is a full slot,
    /// output is `api_dim` floats.
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

    /// Memory layout of one on-storage vector for the given api-level
    /// dimension and metric.
    ///
    /// Default assumes a flat `[Self; api_dim]` layout, ignoring distance.
    /// Types whose encoding depends on the metric (e.g. renormalized
    /// quantization) override this and inspect `distance`.
    fn storage_layout(api_dim: usize, distance: Distance) -> Layout {
        let _ = distance;
        Layout::array::<Self>(api_dim).unwrap()
    }

    /// Length of one on-storage vector slot in `Self`-elements.
    ///
    /// For flat layouts this equals `api_dim`. For types whose
    /// `storage_layout` adds side payload, this is larger.
    fn storage_len_in_elements(api_dim: usize, distance: Distance) -> usize {
        Self::storage_layout(api_dim, distance).size() / size_of::<Self>()
    }

    /// Recover the api-level dimension from a slot length in `Self`-elements.
    ///
    /// Inverse of `storage_len_in_elements`. Default is identity (flat
    /// layout); metric-aware types subtract the payload elements.
    fn api_dim_from_storage_len(storage_len: usize, distance: Distance) -> usize {
        let _ = distance;
        storage_len
    }
}

impl PrimitiveVectorElement for VectorElementType {
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, _distance: Distance) -> Cow<[Self]> {
        vector
    }

    fn slice_to_float_cow(vector: Cow<[Self]>, _distance: Distance) -> Cow<[VectorElementType]> {
        vector
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
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, _distance: Distance) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| f16::from_f32(x)).collect())
    }

    fn slice_to_float_cow(vector: Cow<[Self]>, _distance: Distance) -> Cow<[VectorElementType]> {
        Cow::Owned(vector.iter().map(|&x| f16::to_f32(x)).collect_vec())
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
