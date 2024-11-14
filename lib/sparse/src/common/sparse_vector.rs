use std::borrow::Cow;
use std::hash::Hash;

use blob_store::Blob;
use common::types::ScoreType;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

use crate::common::types::{DimId, DimOffset, DimWeight};

/// Sparse vector structure
#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SparseVector {
    /// Indices must be unique
    pub indices: Vec<DimId>,
    /// Values and indices must be the same length
    pub values: Vec<DimWeight>,
}

/// Same as `SparseVector` but with `DimOffset` indices.
/// Meaning that is uses internal segment-specific indices.
#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize)]
pub struct RemappedSparseVector {
    /// indices must be unique
    pub indices: Vec<DimOffset>,
    /// values and indices must be the same length
    pub values: Vec<DimWeight>,
}

/// Sort two arrays by the first array.
fn double_sort<T: Ord + Copy, V: Copy>(indices: &mut [T], values: &mut [V]) {
    // Check if the indices are already sorted
    if indices.windows(2).all(|w| w[0] < w[1]) {
        return;
    }

    let mut indexed_values: Vec<(T, V)> = indices
        .iter()
        .zip(values.iter())
        .map(|(&i, &v)| (i, v))
        .collect();

    // Sort the vector of tuples by indices
    indexed_values.sort_unstable_by_key(|&(i, _)| i);

    for (i, (index, value)) in indexed_values.into_iter().enumerate() {
        indices[i] = index;
        values[i] = value;
    }
}

fn score_vectors<T: Ord + Eq>(
    self_indices: &[T],
    self_values: &[DimWeight],
    other_indices: &[T],
    other_values: &[DimWeight],
) -> Option<ScoreType> {
    let mut score = 0.0;
    // track whether there is any overlap
    let mut overlap = false;
    let mut i = 0;
    let mut j = 0;
    while i < self_indices.len() && j < other_indices.len() {
        match self_indices[i].cmp(&other_indices[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                overlap = true;
                score += self_values[i] * other_values[j];
                i += 1;
                j += 1;
            }
        }
    }
    if overlap {
        Some(score)
    } else {
        None
    }
}

impl RemappedSparseVector {
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Result<Self, ValidationErrors> {
        let vector = Self { indices, values };
        vector.validate()?;
        Ok(vector)
    }

    pub fn sort_by_indices(&mut self) {
        double_sort(&mut self.indices, &mut self.values);
    }

    /// Check if this vector is sorted by indices.
    pub fn is_sorted(&self) -> bool {
        self.indices.windows(2).all(|w| w[0] < w[1])
    }

    /// Score this vector against another vector using dot product.
    /// Warning: Expects both vectors to be sorted by indices.
    ///
    /// Return None if the vectors do not overlap.
    pub fn score(&self, other: &RemappedSparseVector) -> Option<ScoreType> {
        debug_assert!(self.is_sorted());
        debug_assert!(other.is_sorted());
        score_vectors(&self.indices, &self.values, &other.indices, &other.values)
    }
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Result<Self, ValidationErrors> {
        let vector = SparseVector { indices, values };
        vector.validate()?;
        Ok(vector)
    }

    /// Sort this vector by indices.
    ///
    /// Sorting is required for scoring and overlap checks.
    pub fn sort_by_indices(&mut self) {
        double_sort(&mut self.indices, &mut self.values);
    }

    /// Check if this vector is sorted by indices.
    pub fn is_sorted(&self) -> bool {
        self.indices.windows(2).all(|w| w[0] < w[1])
    }

    /// Check if this vector is empty.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty() && self.values.is_empty()
    }

    /// Score this vector against another vector using dot product.
    /// Warning: Expects both vectors to be sorted by indices.
    ///
    /// Return None if the vectors do not overlap.
    pub fn score(&self, other: &SparseVector) -> Option<ScoreType> {
        debug_assert!(self.is_sorted());
        debug_assert!(other.is_sorted());
        score_vectors(&self.indices, &self.values, &other.indices, &other.values)
    }

    /// Construct a new vector that is the result of performing all indices-wise operations.
    /// Automatically sort input vectors if necessary.
    pub fn combine_aggregate(
        &self,
        other: &SparseVector,
        op: impl Fn(DimWeight, DimWeight) -> DimWeight,
    ) -> Self {
        // Copy and sort `self` vector if not already sorted
        let this: Cow<SparseVector> = if !self.is_sorted() {
            let mut this = self.clone();
            this.sort_by_indices();
            Cow::Owned(this)
        } else {
            Cow::Borrowed(self)
        };
        assert!(this.is_sorted());

        // Copy and sort `other` vector if not already sorted
        let cow_other: Cow<SparseVector> = if !other.is_sorted() {
            let mut other = other.clone();
            other.sort_by_indices();
            Cow::Owned(other)
        } else {
            Cow::Borrowed(other)
        };
        let other = &cow_other;
        assert!(other.is_sorted());

        let mut result = SparseVector::default();
        let mut i = 0;
        let mut j = 0;
        while i < this.indices.len() && j < other.indices.len() {
            match this.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => {
                    result.indices.push(this.indices[i]);
                    result.values.push(op(this.values[i], 0.0));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    result.indices.push(other.indices[j]);
                    result.values.push(op(0.0, other.values[j]));
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    result.indices.push(this.indices[i]);
                    result.values.push(op(this.values[i], other.values[j]));
                    i += 1;
                    j += 1;
                }
            }
        }
        while i < this.indices.len() {
            result.indices.push(this.indices[i]);
            result.values.push(op(this.values[i], 0.0));
            i += 1;
        }
        while j < other.indices.len() {
            result.indices.push(other.indices[j]);
            result.values.push(op(0.0, other.values[j]));
            j += 1;
        }
        debug_assert!(result.is_sorted());
        debug_assert!(result.validate().is_ok());
        result
    }

    /// Create [RemappedSparseVector] from this vector in a naive way. Only suitable for testing.
    #[cfg(feature = "testing")]
    pub fn into_remapped(self) -> RemappedSparseVector {
        RemappedSparseVector {
            indices: self.indices,
            values: self.values,
        }
    }
}

impl TryFrom<Vec<(u32, f32)>> for RemappedSparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<(u32, f32)>) -> Result<Self, Self::Error> {
        let (indices, values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        RemappedSparseVector::new(indices, values)
    }
}

impl TryFrom<Vec<(u32, f32)>> for SparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<(u32, f32)>) -> Result<Self, Self::Error> {
        let (indices, values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        SparseVector::new(indices, values)
    }
}

impl Blob for SparseVector {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Sparse vector serialization should not fail")
    }

    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data).expect("Sparse vector deserialization should not fail")
    }
}

#[cfg(test)]
impl<const N: usize> From<[(u32, f32); N]> for SparseVector {
    fn from(value: [(u32, f32); N]) -> Self {
        value.to_vec().try_into().unwrap()
    }
}

#[cfg(test)]
impl<const N: usize> From<[(u32, f32); N]> for RemappedSparseVector {
    fn from(value: [(u32, f32); N]) -> Self {
        value.to_vec().try_into().unwrap()
    }
}

impl Validate for SparseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_sparse_vector_impl(&self.indices, &self.values)
    }
}

impl Validate for RemappedSparseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_sparse_vector_impl(&self.indices, &self.values)
    }
}

pub fn validate_sparse_vector_impl<T: Clone + Eq + Hash>(
    indices: &[T],
    values: &[DimWeight],
) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrors::default();

    if indices.len() != values.len() {
        errors.add(
            "values",
            ValidationError::new("must be the same length as indices"),
        );
    }
    if indices.iter().unique().count() != indices.len() {
        errors.add("indices", ValidationError::new("must be unique"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_aligned_same_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(14.0));
    }

    #[test]
    fn test_score_not_aligned_same_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![2, 3, 4], vec![2.0, 3.0, 4.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(13.0));
    }

    #[test]
    fn test_score_aligned_different_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![1, 2, 3, 4], vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(14.0));
    }

    #[test]
    fn test_score_not_aligned_different_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![2, 3, 4, 5], vec![2.0, 3.0, 4.0, 5.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(13.0));
    }

    #[test]
    fn test_score_no_overlap() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![4, 5, 6], vec![2.0, 3.0, 4.0]).unwrap();
        assert!(v1.score(&v2).is_none());
    }

    #[test]
    fn validation_test() {
        let fully_empty = SparseVector::new(vec![], vec![]);
        assert!(fully_empty.is_ok());
        assert!(fully_empty.unwrap().is_empty());

        let different_length = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0]);
        assert!(different_length.is_err());

        let not_sorted = SparseVector::new(vec![1, 3, 2], vec![1.0, 2.0, 3.0]);
        assert!(not_sorted.is_ok());

        let not_unique = SparseVector::new(vec![1, 2, 3, 2], vec![1.0, 2.0, 3.0, 4.0]);
        assert!(not_unique.is_err());
    }

    #[test]
    fn sorting_test() {
        let mut not_sorted = SparseVector::new(vec![1, 3, 2], vec![1.0, 2.0, 3.0]).unwrap();
        assert!(!not_sorted.is_sorted());
        not_sorted.sort_by_indices();
        assert!(not_sorted.is_sorted());
    }

    #[test]
    fn combine_aggregate_test() {
        // Test with missing index
        let a = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]).unwrap();
        let b = SparseVector::new(vec![2, 3, 4], vec![2.0, 3.0, 4.0]).unwrap();
        let sum = a.combine_aggregate(&b, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.1, 4.2, 6.3, 8.0]);

        // reverse arguments
        let sum = b.combine_aggregate(&a, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.2, 2.4, 3.6, 4.0]);

        // Test with non-sorted input
        let a = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]).unwrap();
        let b = SparseVector::new(vec![4, 2, 3], vec![4.0, 2.0, 3.0]).unwrap();
        let sum = a.combine_aggregate(&b, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.1, 4.2, 6.3, 8.0]);
    }
}
