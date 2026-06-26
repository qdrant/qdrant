use std::borrow::Cow;
use std::collections::HashMap;

use sparse::common::sparse_vector::SparseVector;

use super::primitive::PrimitiveVectorElement;
use super::tiny_map;
use super::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, TypedMultiDenseVectorRef,
    VectorElementType, VectorElementTypeByte, VectorElementTypeHalf, VectorInternal, VectorRef,
};
use crate::common::operation_error::OperationError;
use crate::types::{Distance, VectorDataConfig, VectorName, VectorNameBuf, VectorStorageDatatype};

type CowKey<'a> = Cow<'a, VectorName>;

#[derive(Clone, PartialEq, Debug)]
pub enum CowMultiVector<'a, TElement: PrimitiveVectorElement> {
    Owned(TypedMultiDenseVector<TElement>),
    Borrowed(TypedMultiDenseVectorRef<'a, TElement>),
}

impl<TElement> CowMultiVector<'_, TElement>
where
    TElement: PrimitiveVectorElement,
{
    pub fn as_ref(&self) -> TypedMultiDenseVectorRef<'_, TElement> {
        match self {
            CowMultiVector::Owned(owned) => TypedMultiDenseVectorRef {
                flattened_vectors: &owned.flattened_vectors,
                dim: owned.dim,
            },
            CowMultiVector::Borrowed(borrowed) => *borrowed,
        }
    }

    fn flattened_len(&self) -> usize {
        match self {
            CowMultiVector::Owned(typed_multi_dense_vector) => {
                typed_multi_dense_vector.flattened_len()
            }
            CowMultiVector::Borrowed(typed_multi_dense_vector_ref) => {
                typed_multi_dense_vector_ref.flattened_len()
            }
        }
    }
}

/// A storage-native quantized vector carried as opaque encoded bytes plus the
/// local context needed to decode it (TurboQuant is fully deterministic given
/// `dim` + `distance`). This is the in-memory representation that lets a
/// quantized vector travel through `NamedVectors` / CoW / clone-and-mutate
/// without a dequantize -> re-quantize round-trip; only the TQ storage's write
/// path consumes the bytes verbatim, every other consumer dequantizes on demand.
///
/// `dim` / `distance` are local context (from the owning or target storage).
/// Equality is by all fields, so comparing two `Quantized` is a byte compare —
/// exactly the "unchanged" check a copy-on-write needs.
///
/// Handles both dense single vectors and multivectors. `multivector_count`
/// distinguishes them: `None` is a dense single (`dim` floats); `Some(n)` is a
/// multivector of `n` inner vectors, the bytes being `n` concatenated inner
/// encodings of `dim` floats each. `dim` is always the inner-vector dimension.
///
/// There is no encoding-version field: within a single node every blob is
/// produced and consumed by the same process with the same TQDT constants, so a
/// version would always equal itself. Versioning only matters when bytes cross a
/// node/build boundary, where it rides the wire type separately.
#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub struct CowQuantizedVector<'a> {
    pub bytes: Cow<'a, [u8]>,
    pub dim: usize,
    pub distance: Distance,
    /// `None` for a dense single vector; `Some(inner_count)` for a multivector.
    pub multivector_count: Option<usize>,
}

impl CowQuantizedVector<'_> {
    /// Decode back to a plain float vector: dense for a single, multi for a
    /// multivector (TurboQuant).
    pub fn dequantize(&self) -> VectorInternal {
        match self.multivector_count {
            None => VectorInternal::Dense(crate::vector_storage::turbo::dequantize_tqdt_dense(
                &self.bytes,
                self.dim,
                self.distance,
            )),
            Some(count) => {
                VectorInternal::MultiDense(crate::vector_storage::turbo::dequantize_tqdt_multi(
                    &self.bytes,
                    self.dim,
                    self.distance,
                    count,
                ))
            }
        }
    }

    pub fn into_owned(self) -> CowQuantizedVector<'static> {
        CowQuantizedVector {
            bytes: Cow::Owned(self.bytes.into_owned()),
            dim: self.dim,
            distance: self.distance,
            multivector_count: self.multivector_count,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum CowVector<'a> {
    Dense(Cow<'a, [VectorElementType]>),
    Sparse(Cow<'a, SparseVector>),
    MultiDense(CowMultiVector<'a, VectorElementType>),
    /// Storage-native quantized bytes (e.g. TurboQuant), decoded on demand by
    /// any float consumer; preserved verbatim only by a matching TQ storage.
    Quantized(CowQuantizedVector<'a>),
}

impl Default for CowVector<'_> {
    fn default() -> Self {
        CowVector::Dense(Cow::Owned(Vec::new()))
    }
}

impl CowVector<'_> {
    pub fn estimate_size_in_bytes(&self) -> usize {
        match self {
            CowVector::Dense(cow) => cow.len() * size_of::<VectorElementType>(),
            CowVector::Sparse(cow) => cow.indices.len() * size_of::<VectorElementType>() * 2, // indices & values
            CowVector::MultiDense(cow_multi_vector) => {
                cow_multi_vector.flattened_len() * size_of::<VectorElementType>()
            }
            CowVector::Quantized(q) => q.bytes.len(),
        }
    }
}

type TinyMap<'a> = tiny_map::TinyMap<CowKey<'a>, CowVector<'a>>;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct NamedVectors<'a> {
    map: TinyMap<'a>,
}

impl<'a, TElement: PrimitiveVectorElement> CowMultiVector<'a, TElement> {
    pub fn to_owned(self) -> TypedMultiDenseVector<TElement> {
        match self {
            CowMultiVector::Owned(v) => v,
            CowMultiVector::Borrowed(v) => v.to_owned(),
        }
    }

    pub fn as_vec_ref(&'a self) -> TypedMultiDenseVectorRef<'a, TElement> {
        match self {
            CowMultiVector::Owned(v) => TypedMultiDenseVectorRef {
                flattened_vectors: &v.flattened_vectors,
                dim: v.dim,
            },
            CowMultiVector::Borrowed(v) => *v,
        }
    }
}

impl CowVector<'_> {
    pub fn default_sparse() -> Self {
        CowVector::Sparse(Cow::Owned(SparseVector::default()))
    }

    /// Own the vector **preserving its native encoding** (a `Quantized` becomes
    /// [`VectorInternal::Quantized`], not a dequantized float). `VectorInternal`
    /// is "maybe-encoded": consumers that need floats decode it explicitly (the
    /// compiler forces them at each match / `From`/`TryFrom`).
    pub fn to_owned(self) -> VectorInternal {
        match self {
            CowVector::Dense(v) => VectorInternal::Dense(v.into_owned()),
            CowVector::Sparse(v) => VectorInternal::Sparse(v.into_owned()),
            CowVector::MultiDense(v) => VectorInternal::MultiDense(v.to_owned()),
            CowVector::Quantized(q) => VectorInternal::Quantized(q.into_owned()),
        }
    }

    /// Own any borrowed data **without dequantizing**: a `Quantized` stays
    /// `Quantized` (with owned bytes), unlike [`Self::to_owned`] which decodes to
    /// a float `VectorInternal`. Used to carry a vector across a storage borrow
    /// while preserving its native encoding.
    pub fn into_owned(self) -> CowVector<'static> {
        match self {
            CowVector::Dense(v) => CowVector::Dense(Cow::Owned(v.into_owned())),
            CowVector::Sparse(v) => CowVector::Sparse(Cow::Owned(v.into_owned())),
            CowVector::MultiDense(v) => CowVector::MultiDense(CowMultiVector::Owned(v.to_owned())),
            CowVector::Quantized(q) => CowVector::Quantized(q.into_owned()),
        }
    }

    pub fn as_vec_ref(&self) -> VectorRef<'_> {
        match self {
            CowVector::Dense(v) => VectorRef::Dense(v.as_ref()),
            CowVector::Sparse(v) => VectorRef::Sparse(v.as_ref()),
            CowVector::MultiDense(v) => VectorRef::MultiDense(v.as_vec_ref()),
            CowVector::Quantized(q) => VectorRef::Quantized(q),
        }
    }
}

impl<'a> From<Cow<'a, [VectorElementType]>> for CowVector<'a> {
    fn from(v: Cow<'a, [VectorElementType]>) -> Self {
        match v {
            Cow::Borrowed(v) => CowVector::Dense(Cow::Borrowed(v)),
            Cow::Owned(v) => CowVector::Dense(Cow::Owned(v)),
        }
    }
}

impl From<VectorInternal> for CowVector<'_> {
    fn from(v: VectorInternal) -> Self {
        match v {
            VectorInternal::Dense(v) => CowVector::Dense(Cow::Owned(v)),
            VectorInternal::Sparse(v) => CowVector::Sparse(Cow::Owned(v)),
            VectorInternal::MultiDense(v) => CowVector::MultiDense(CowMultiVector::Owned(v)),
            VectorInternal::Quantized(q) => CowVector::Quantized(q),
        }
    }
}

impl From<SparseVector> for CowVector<'_> {
    fn from(v: SparseVector) -> Self {
        CowVector::Sparse(Cow::Owned(v))
    }
}

impl From<DenseVector> for CowVector<'_> {
    fn from(v: DenseVector) -> Self {
        CowVector::Dense(Cow::Owned(v))
    }
}

impl From<MultiDenseVectorInternal> for CowVector<'_> {
    fn from(v: MultiDenseVectorInternal) -> Self {
        CowVector::MultiDense(CowMultiVector::Owned(v))
    }
}

impl<'a> From<Cow<'a, MultiDenseVectorInternal>> for CowVector<'a> {
    fn from(v: Cow<'a, MultiDenseVectorInternal>) -> Self {
        match v {
            Cow::Borrowed(v) => {
                CowVector::MultiDense(CowMultiVector::Borrowed(TypedMultiDenseVectorRef::from(v)))
            }
            Cow::Owned(v) => CowVector::MultiDense(CowMultiVector::Owned(v)),
        }
    }
}

impl<'a> From<&'a SparseVector> for CowVector<'a> {
    fn from(v: &'a SparseVector) -> Self {
        CowVector::Sparse(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a [VectorElementType]> for CowVector<'a> {
    fn from(v: &'a [VectorElementType]) -> Self {
        CowVector::Dense(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a MultiDenseVectorInternal> for CowVector<'a> {
    fn from(v: &'a MultiDenseVectorInternal) -> Self {
        CowVector::MultiDense(CowMultiVector::Borrowed(TypedMultiDenseVectorRef::from(v)))
    }
}

impl<'a> TryFrom<CowVector<'a>> for SparseVector {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(_) => Err(OperationError::WrongSparse),
            CowVector::Sparse(v) => Ok(v.into_owned()),
            CowVector::MultiDense(_) => Err(OperationError::WrongSparse),
            CowVector::Quantized(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl<'a> TryFrom<CowVector<'a>> for DenseVector {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(v) => Ok(v.into_owned()),
            CowVector::Sparse(_) => Err(OperationError::WrongSparse),
            CowVector::MultiDense(_) => Err(OperationError::WrongMulti),
            CowVector::Quantized(q) => match q.dequantize() {
                VectorInternal::Dense(v) => Ok(v),
                VectorInternal::Sparse(_)
                | VectorInternal::MultiDense(_)
                | VectorInternal::Quantized(_) => Err(OperationError::WrongMulti),
            },
        }
    }
}

impl<'a> TryFrom<CowVector<'a>> for Cow<'a, [VectorElementType]> {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(v) => Ok(v),
            CowVector::Sparse(_) => Err(OperationError::WrongSparse),
            CowVector::MultiDense(_) => Err(OperationError::WrongMulti),
            CowVector::Quantized(q) => match q.dequantize() {
                VectorInternal::Dense(v) => Ok(Cow::Owned(v)),
                VectorInternal::Sparse(_)
                | VectorInternal::MultiDense(_)
                | VectorInternal::Quantized(_) => Err(OperationError::WrongMulti),
            },
        }
    }
}

impl<'a> From<VectorRef<'a>> for CowVector<'a> {
    fn from(v: VectorRef<'a>) -> Self {
        match v {
            VectorRef::Dense(v) => CowVector::Dense(Cow::Borrowed(v)),
            VectorRef::Sparse(v) => CowVector::Sparse(Cow::Borrowed(v)),
            VectorRef::MultiDense(v) => CowVector::MultiDense(CowMultiVector::Borrowed(v)),
            VectorRef::Quantized(q) => CowVector::Quantized(q.clone()),
        }
    }
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a VectorName, value: VectorRef<'a>) -> Self {
        let mut map = TinyMap::new();
        map.insert(Cow::Borrowed(key), CowVector::from(value));
        Self { map }
    }

    pub fn from_pairs<const N: usize>(arr: [(VectorNameBuf, DenseVector); N]) -> Self {
        NamedVectors {
            map: arr
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowVector::Dense(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn merge(&mut self, other: NamedVectors<'a>) {
        for (key, value) in other {
            self.map.insert(key, value);
        }
    }

    pub fn insert(&mut self, name: VectorNameBuf, vector: VectorInternal) {
        self.map
            .insert(CowKey::Owned(name), CowVector::from(vector));
    }

    /// Insert an already-built [`CowVector`], preserving a
    /// [`CowVector::Quantized`] (unlike [`Self::insert`], which takes an owned
    /// float `VectorInternal`). Used by the native read path.
    pub fn insert_cow(&mut self, name: VectorNameBuf, vector: CowVector<'a>) {
        self.map.insert(CowKey::Owned(name), vector);
    }

    pub fn remove_ref(&mut self, key: &VectorName) {
        self.map.remove(key);
    }

    pub fn contains_key(&self, key: &VectorName) -> bool {
        self.map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &VectorName> {
        self.map.iter().map(|(k, _)| k.as_ref())
    }

    /// Iterate `(name, &CowVector)` without borrowing a float view, so it is
    /// safe in the presence of [`CowVector::Quantized`] (unlike [`Self::iter`],
    /// which calls `as_vec_ref`).
    pub fn iter_cow(&self) -> impl Iterator<Item = (&VectorName, &CowVector<'a>)> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v))
    }

    pub fn into_owned_map(self) -> HashMap<VectorNameBuf, VectorInternal> {
        self.map
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.to_owned()))
            .collect()
    }

    /// Materialise into a fully-owned `NamedVectors<'static>` by cloning any
    /// borrowed keys/values into owned ones, **preserving native encoding** (a
    /// `Quantized` vector is not dequantized — see [`CowVector::into_owned`]).
    pub fn into_owned(self) -> NamedVectors<'static> {
        let mut out = NamedVectors::default();
        for (name, vector) in self {
            out.insert_cow(name.into_owned(), vector.into_owned());
        }
        out
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VectorName, VectorRef<'_>)> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_vec_ref()))
    }

    pub fn get(&self, key: &VectorName) -> Option<VectorRef<'_>> {
        self.map.get(key).map(|v| v.as_vec_ref())
    }

    /// Like [`Self::get`] but returns the owning [`CowVector`], which (unlike a
    /// borrowed `VectorRef`) can be a [`CowVector::Quantized`]. The write path
    /// uses this so it can route quantized vectors without dequantizing.
    pub fn get_cow(&self, key: &VectorName) -> Option<&CowVector<'_>> {
        self.map.get(key)
    }

    pub fn preprocess<'b>(
        &mut self,
        get_vector_data: impl Fn(&VectorName) -> &'b VectorDataConfig,
    ) {
        for (name, vector) in self.map.iter_mut() {
            match vector {
                CowVector::Dense(v) => {
                    let config = get_vector_data(name.as_ref());
                    let preprocessed_vector = Self::preprocess_dense_vector(v.to_vec(), config);
                    *vector = CowVector::Dense(Cow::Owned(preprocessed_vector))
                }
                CowVector::Sparse(v) => {
                    // sort by indices to enable faster dot product and overlap checks
                    if !v.is_sorted() {
                        v.to_mut().sort_by_indices();
                    }
                }
                CowVector::MultiDense(multi_vector) => {
                    // invalid temp value to swap with multi_vector and reduce reallocations
                    let mut tmp_multi_vector = CowMultiVector::Borrowed(TypedMultiDenseVectorRef {
                        flattened_vectors: &[],
                        dim: 1,
                    });
                    // `multi_vector` is empty invalid and `tmp_multi_vector` owns the real data
                    std::mem::swap(&mut tmp_multi_vector, multi_vector);
                    let mut owned_multi_vector = tmp_multi_vector.to_owned();
                    let config = get_vector_data(name.as_ref());
                    for dense_vector in owned_multi_vector.multi_vectors_mut() {
                        let preprocessed_vector =
                            Self::preprocess_dense_vector(dense_vector.to_vec(), config);
                        // replace dense vector with preprocessed vector
                        dense_vector.copy_from_slice(&preprocessed_vector);
                    }
                    *multi_vector = CowMultiVector::Owned(owned_multi_vector);
                }
                // Preprocess chokepoint: a quantized vector is already final encoded
                // bytes (the original float was normalized/cast before quantizing),
                // so normalization/datatype casting must NOT touch it. Pass through.
                CowVector::Quantized(_) => {}
            }
        }
    }

    fn preprocess_dense_vector(
        dense_vector: DenseVector,
        config: &VectorDataConfig,
    ) -> DenseVector {
        match config.datatype {
            Some(VectorStorageDatatype::Float32) | None => config
                .distance
                .preprocess_vector::<VectorElementType>(dense_vector),
            Some(VectorStorageDatatype::Uint8) => config
                .distance
                .preprocess_vector::<VectorElementTypeByte>(dense_vector),
            Some(VectorStorageDatatype::Float16) => config
                .distance
                .preprocess_vector::<VectorElementTypeHalf>(dense_vector),
            Some(VectorStorageDatatype::Turbo4) => config
                .distance
                .preprocess_vector::<VectorElementType>(dense_vector), // Turbo only needs normal preprocessing.
        }
    }
}

impl<'a> IntoIterator for NamedVectors<'a> {
    type Item = (CowKey<'a>, CowVector<'a>);

    type IntoIter =
        tinyvec::TinyVecIterator<[(CowKey<'a>, CowVector<'a>); super::tiny_map::CAPACITY]>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
