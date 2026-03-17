use std::collections::HashMap;

use segment::common::operation_error::OperationError;
use segment::data_types::vectors::{
    MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;

/// Single vector.
///
/// ```
/// use edge::Vector;
///
/// // Constructor
/// let dense = Vector::new_dense(vec![1.0, 2.0, 3.0]);
/// let sparse = Vector::new_sparse(vec![0, 2], vec![1.0, 3.0])?;
/// let multi = Vector::new_multi(vec![vec![1.0, 2.0], vec![3.0, 4.0]])?;
///
/// // From conversions
/// let dense: Vector = vec![1.0, 2.0, 3.0].into();
/// let multi: Vector = vec![vec![1.0, 2.0], vec![3.0, 4.0]].try_into()?;
///
/// // TryFrom for sparse
/// let sparse = Vector::try_from(vec![(0u32, 1.0), (2, 3.0)])?;
/// # Ok::<_, edge::OperationError>(())
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Vector(pub VectorInternal);

/// Collection of vectors for a point.
///
/// ```
/// use std::collections::HashMap;
/// use edge::{Vector, Vectors};
///
/// // Constructor
/// let named = Vectors::new_named([("text", vec![1.0, 2.0]), ("image", vec![3.0, 4.0])]);
///
/// // From conversions
/// let single: Vectors = vec![1.0, 2.0].into();
/// let multi: Vectors = vec![vec![1.0, 2.0], vec![3.0, 4.0]].try_into()?;
/// let named: Vectors = HashMap::from([("text".to_string(), vec![1.0, 2.0])]).into();
/// let named: Vectors = HashMap::from([("text".to_string(), Vector::new_dense(vec![1.0]))]).into();
/// # Ok::<_, edge::OperationError>(())
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Vectors(pub VectorStructInternal);

/// Named vectors map.
#[derive(Debug, Clone, Default)]
pub struct NamedVectors(pub HashMap<String, Vector>);

impl Vector {
    pub fn new_dense(values: impl Into<Vec<f32>>) -> Self {
        Self(VectorInternal::Dense(values.into()))
    }

    pub fn new_sparse(
        indices: impl Into<Vec<DimId>>,
        values: impl Into<Vec<f32>>,
    ) -> Result<Self, OperationError> {
        SparseVector::new(indices.into(), values.into())
            .map(|v| Self(VectorInternal::Sparse(v)))
            .map_err(|e| OperationError::validation_error(e.to_string()))
    }

    pub fn new_multi(vectors: impl Into<Vec<Vec<f32>>>) -> Result<Self, OperationError> {
        MultiDenseVectorInternal::try_from_matrix(vectors.into())
            .map(|v| Self(VectorInternal::MultiDense(v)))
    }
}

impl From<Vec<f32>> for Vector {
    fn from(v: Vec<f32>) -> Self {
        Self::new_dense(v)
    }
}

impl From<&[f32]> for Vector {
    fn from(v: &[f32]) -> Self {
        Self::new_dense(v.to_vec())
    }
}

impl TryFrom<Vec<(u32, f32)>> for Vector {
    type Error = OperationError;

    fn try_from(tuples: Vec<(u32, f32)>) -> Result<Self, Self::Error> {
        Self::try_from(tuples.as_slice())
    }
}

impl TryFrom<&[(u32, f32)]> for Vector {
    type Error = OperationError;

    fn try_from(tuples: &[(u32, f32)]) -> Result<Self, Self::Error> {
        let (indices, values): (Vec<_>, Vec<_>) = tuples.iter().cloned().unzip();
        Self::new_sparse(indices, values)
    }
}

impl TryFrom<Vec<Vec<f32>>> for Vector {
    type Error = OperationError;

    fn try_from(vectors: Vec<Vec<f32>>) -> Result<Self, Self::Error> {
        Self::new_multi(vectors)
    }
}

impl From<VectorInternal> for Vector {
    fn from(v: VectorInternal) -> Self {
        Self(v)
    }
}

impl From<Vector> for VectorInternal {
    fn from(v: Vector) -> Self {
        v.0
    }
}

impl From<SparseVector> for Vector {
    fn from(v: SparseVector) -> Self {
        Self(VectorInternal::Sparse(v))
    }
}

impl From<MultiDenseVectorInternal> for Vector {
    fn from(v: MultiDenseVectorInternal) -> Self {
        Self(VectorInternal::MultiDense(v))
    }
}

impl Vectors {
    pub fn new_named(
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<Vector>)>,
    ) -> Self {
        Self(VectorStructInternal::Named(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into().0))
                .collect(),
        ))
    }
}

impl From<Vec<f32>> for Vectors {
    fn from(v: Vec<f32>) -> Self {
        Self(VectorStructInternal::Single(v))
    }
}

impl From<&[f32]> for Vectors {
    fn from(v: &[f32]) -> Self {
        Self(VectorStructInternal::Single(v.to_vec()))
    }
}

impl TryFrom<Vec<Vec<f32>>> for Vectors {
    type Error = OperationError;

    fn try_from(vectors: Vec<Vec<f32>>) -> Result<Self, Self::Error> {
        Vector::new_multi(vectors).map(Self::from)
    }
}

impl From<Vector> for Vectors {
    fn from(vector: Vector) -> Self {
        match vector.0 {
            VectorInternal::Dense(v) => Self(VectorStructInternal::Single(v)),
            VectorInternal::MultiDense(v) => Self(VectorStructInternal::MultiDense(v)),
            VectorInternal::Sparse(v) => Self(VectorStructInternal::Named(
                [(String::new(), VectorInternal::Sparse(v))].into(),
            )),
        }
    }
}

impl From<HashMap<String, Vec<f32>>> for Vectors {
    fn from(named: HashMap<String, Vec<f32>>) -> Self {
        Self(VectorStructInternal::Named(
            named
                .into_iter()
                .map(|(k, v)| (k, VectorInternal::Dense(v)))
                .collect(),
        ))
    }
}

impl From<HashMap<String, Vector>> for Vectors {
    fn from(named: HashMap<String, Vector>) -> Self {
        Self(VectorStructInternal::Named(
            named.into_iter().map(|(k, v)| (k, v.0)).collect(),
        ))
    }
}

impl<T: Into<String>, V: Into<Vector>> From<Vec<(T, V)>> for Vectors {
    fn from(pairs: Vec<(T, V)>) -> Self {
        Self(VectorStructInternal::Named(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into().0))
                .collect(),
        ))
    }
}

impl From<VectorStructInternal> for Vectors {
    fn from(v: VectorStructInternal) -> Self {
        Self(v)
    }
}

impl From<Vectors> for VectorStructInternal {
    fn from(v: Vectors) -> Self {
        v.0
    }
}

impl NamedVectors {
    pub fn add_vector(mut self, name: impl Into<String>, vector: impl Into<Vector>) -> Self {
        self.0.insert(name.into(), vector.into());
        self
    }
}

impl From<NamedVectors> for Vectors {
    fn from(named: NamedVectors) -> Self {
        Self(VectorStructInternal::Named(
            named.0.into_iter().map(|(k, v)| (k, v.0)).collect(),
        ))
    }
}

impl From<HashMap<String, Vector>> for NamedVectors {
    fn from(map: HashMap<String, Vector>) -> Self {
        Self(map)
    }
}

impl<T: Into<String>, V: Into<Vector>> From<Vec<(T, V)>> for NamedVectors {
    fn from(pairs: Vec<(T, V)>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}
