use std::borrow::Cow;
use std::convert::Infallible;
use std::ops::Range;
use std::path::PathBuf;

use common::ambient::hw::HwMetric;
use common::mmap::Flusher;
use common::typelevel::TBool;
use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

use crate::EncodingError;
use crate::max_sim::MultiQuery;
use crate::packed_multi_query::PackedMultiQuery;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceType {
    // Warning!
    // Old Qdrant versions used `Dot` for both Cosine and Dot (since their implementations were equal).
    // However, TurboQuant needs to know the exact distance type and can't treat Cosine and Dot equally.
    // Because this distinction was introduced together with TQ, quantization storages created prior to
    // TQ might still store `Dot` even though the original vectors use `Cosine`.
    // Therefore, we can't rely on the exact type for any quantization storage other than TQ, and must *always* treat
    // Cosine and Dot the same for quantization types that were implemented prior to TQ.
    Cosine,

    Dot,
    L1,
    L2,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct VectorParameters {
    pub dim: usize,
    pub distance_type: DistanceType,
    pub invert: bool,

    // Deprecated, use `EncodedVectors::vectors_count` from quantization instead.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "count")]
    pub deprecated_count: Option<usize>,
}

pub trait EncodedVectors: Sized {
    type EncodedQuery;

    fn is_in_ram_or_mmap() -> bool;
    /// Whether the encoded data was opened cold, see [`EncodedStorageWrite::is_cold`].
    ///
    /// [`EncodedStorageWrite::is_cold`]: crate::encoded_storage::EncodedStorageWrite::is_cold
    fn is_cold(&self) -> bool;

    fn encode_query(&self, query: &[f32]) -> Self::EncodedQuery;

    /// This function is expected to:
    /// - be implemented by non-multivector storages
    /// - be used by multivector storages
    fn for_each_batch(
        &self,
        offsets: &[PointOffsetType],
        callback: impl FnMut(usize, Cow<'_, [u8]>),
    ) -> std::io::Result<()>;

    fn score(&self, query: &Self::EncodedQuery, encoded_vector: &[u8]) -> f32;

    /// Pack a multi-vector query, one encoded query per inner vector, for a
    /// batched MaxSim kernel over this storage's vectors, as a [`MultiQuery`]
    /// holds it. Scores are those of [`Self::score`].
    ///
    /// `None` when there is no batched kernel for this storage; callers then
    /// score pair by pair.
    fn pack_multi_query(&self, _query: &[Self::EncodedQuery]) -> Option<PackedMultiQuery> {
        None
    }

    /// Pack the stored vectors of one multi-vector, as [`EncodedStorage`] holds
    /// them, as the query side of a batched MaxSim against other stored vectors.
    /// Scores are those of [`Self::score_internal`].
    ///
    /// `None` when there is no batched kernel for this storage.
    ///
    /// [`EncodedStorage`]: crate::encoded_storage::EncodedStorage
    fn pack_multi_stored<'a>(
        &self,
        _vectors: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> Option<PackedMultiQuery> {
        None
    }

    /// Whether the packed kernels score a multi-vector of `vectors_count`
    /// vectors, packed as a query or (`stored`) as stored vectors, faster than
    /// pair by pair on this CPU. See [`crossover`].
    ///
    /// [`crossover`]: crate::packed_multi_query::crossover
    fn packed_pays(&self, _vectors_count: usize, _stored: bool) -> bool {
        false
    }

    /// [`Self::pack_multi_query`] when it is faster than scoring pair by pair.
    fn pack_multi_query_if_faster(&self, query: &[Self::EncodedQuery]) -> Option<PackedMultiQuery> {
        if !self.packed_pays(query.len(), false) {
            return None;
        }
        self.pack_multi_query(query)
            .filter(|packed| !packed.is_slow())
    }

    /// [`Self::pack_multi_stored`] when it is faster than scoring pair by pair.
    fn pack_multi_stored_if_faster<'a>(
        &self,
        vectors: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> Option<PackedMultiQuery> {
        if !self.packed_pays(vectors.len(), true) {
            return None;
        }
        self.pack_multi_stored(vectors)
            .filter(|packed| !packed.is_slow())
    }

    /// Prepare a multi-vector query, one encoded query per inner vector, for
    /// MaxSim against this storage's vectors: packed when a batched kernel is
    /// faster ([`Self::pack_multi_query_if_faster`]), else scored pair by pair
    /// with [`Self::score`].
    fn encode_multi_query(&self, query: Vec<Self::EncodedQuery>) -> MultiQuery<Self::EncodedQuery> {
        match self.pack_multi_query_if_faster(&query) {
            Some(packed) => MultiQuery::Packed(packed),
            None => MultiQuery::PerPair(query),
        }
    }

    /// MaxSim between two stored multi-vectors, given by the offsets of their
    /// vectors: for each vector of `a`, its best [`Self::score_internal`] against
    /// the vectors of `b`, summed. The vectors of `a` are packed when a batched
    /// kernel is faster ([`Self::pack_multi_stored_if_faster`]).
    fn score_internal_max_sim(&self, a: Range<PointOffsetType>, b: Range<PointOffsetType>) -> f32 {
        if let Some(score) = packed_internal_max_sim(self, a.clone(), b.clone()) {
            return score;
        }
        let mut sum = 0.0;
        for i in a {
            let mut max_sim = f32::NEG_INFINITY;
            // manual `max_by` for performance
            for j in b.clone() {
                let sim = self.score_internal(i, j);
                if sim > max_sim {
                    max_sim = sim;
                }
            }
            sum += max_sim;
        }
        sum
    }

    /// Score a batch: `scores[i]` ← score of `query` against point
    /// `offsets[i]`.
    ///
    /// The default is the per-vector loop the scorers have always run.
    /// Implementations can batch harder — resolve contiguous storage runs
    /// once and score each run with a single kernel call instead of paying
    /// the per-point machinery for every vector.
    fn score_points(
        &self,
        query: &Self::EncodedQuery,
        offsets: &[PointOffsetType],
        scores: &mut [f32],
    ) -> std::io::Result<()> {
        debug_assert_eq!(offsets.len(), scores.len());
        self.for_each_batch(offsets, |i, vector| {
            scores[i] = self.score(query, &vector);
        })
    }

    fn score_internal(&self, i: PointOffsetType, j: PointOffsetType) -> f32;

    /// Return size in bytes of a quantized vector
    fn quantized_vector_size(&self) -> usize;

    /// Construct a query from stored vector, so it can be used for scoring.
    /// Some implementations may not support this, in which case they should return `None`.
    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<Self::EncodedQuery>;

    fn upsert_vector(&mut self, id: PointOffsetType, vector: &[f32]) -> std::io::Result<()>;

    fn vectors_count(&self) -> usize;

    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    /// Additional heap memory used by this encoded vectors instance
    /// beyond what's tracked in files (storage heap + metadata).
    ///
    /// Required (no default) so every quantizer must account for its own
    /// heap explicitly — at minimum delegating to the storage backend's
    /// [`EncodedStorage::heap_size_bytes`].
    fn heap_size_bytes(&self) -> usize;

    type SupportsBytes: TBool;
    fn score_bytes(
        &self,
        enabled: Self::SupportsBytes,
        query: &Self::EncodedQuery,
        bytes: &[u8],
    ) -> f32;
}

impl DistanceType {
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceType::Dot | DistanceType::Cosine => a.iter().zip(b).map(|(a, b)| a * b).sum(),
            DistanceType::L1 => a.iter().zip(b).map(|(a, b)| (a - b).abs()).sum(),
            DistanceType::L2 => a.iter().zip(b).map(|(a, b)| (a - b) * (a - b)).sum(),
        }
    }
}

/// The vectors of `data` to fit the quantization parameters on, and how many there are.
///
/// `keep` holds one flag per vector of `data`, set for the vectors to fit on.
/// [`EncodedVectors::score_internal_max_sim`] with the vectors of `a` packed;
/// `None` without a faster batched kernel, or when the vectors can't be read.
fn packed_internal_max_sim<E: EncodedVectors>(
    storage: &E,
    a: Range<PointOffsetType>,
    b: Range<PointOffsetType>,
) -> Option<f32> {
    if !E::is_in_ram_or_mmap() || !storage.packed_pays(a.len(), true) {
        return None;
    }
    let a: Vec<_> = a.collect();
    let b: Vec<_> = b.collect();
    let vector_size = storage.quantized_vector_size();
    let mut vectors_a = Vec::with_capacity(a.len() * vector_size);
    storage
        .for_each_batch(&a, |_, vector| vectors_a.extend_from_slice(&vector))
        .ok()?;
    let packed = storage.pack_multi_stored_if_faster(vectors_a.chunks_exact(vector_size))?;
    // Packed, so it never scores a pair: no encoded queries to score.
    let query = MultiQuery::<Infallible>::Packed(packed);
    let mut max_sim = query.max_sim(|never, _| match *never {});
    storage
        .for_each_batch(&b, |_, vector| max_sim.push(&vector))
        .ok()?;
    let mul = usize::from(storage.is_cold()); // Reads from RAM don't count as IO.
    HwMetric::VectorIoRead.bump((a.len() + b.len()) * vector_size * mul);
    Some(max_sim.finish())
}

pub(crate) fn calibration_vectors<V>(
    data: impl Iterator<Item = V> + Clone,
    keep: impl Iterator<Item = bool> + Clone,
) -> (impl Iterator<Item = V> + Clone, usize) {
    debug_assert_eq!(
        keep.clone().count(),
        data.clone().count(),
        "calibration mask must have one flag per vector",
    );
    let count = keep.clone().filter(|&keep| keep).count();
    let vectors = data
        .zip(keep)
        .filter_map(|(vector, keep)| keep.then_some(vector));
    (vectors, count)
}

pub(crate) fn validate_vector_parameters<'a>(
    data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    vector_parameters: &VectorParameters,
) -> Result<(), EncodingError> {
    let mut count = 0;
    for vector in data {
        let vector = vector.as_ref();
        if vector.len() != vector_parameters.dim {
            return Err(EncodingError::ArgumentsError(format!(
                "Vector length {} does not match vector parameters dim {}",
                vector.len(),
                vector_parameters.dim
            )));
        }
        count += 1;
    }
    if let Some(vectors_count) = vector_parameters.deprecated_count
        && count != vectors_count
    {
        return Err(EncodingError::ArgumentsError(format!(
            "Vector count {count} does not match vector parameters count {vectors_count}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calibration_vectors() {
        let data = [[1.0f32], [2.0], [3.0]];
        let (vectors, count) = calibration_vectors(data.iter(), [true, false, true].into_iter());
        assert_eq!(count, 2);
        assert_eq!(vectors.collect::<Vec<_>>(), [&[1.0], &[3.0]]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "calibration mask must have one flag per vector")]
    fn test_calibration_vectors_rejects_short_mask() {
        let data = [[1.0f32], [2.0], [3.0]];
        let (vectors, _) = calibration_vectors(data.iter(), [true, true].into_iter());
        vectors.for_each(drop);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "calibration mask must have one flag per vector")]
    fn test_calibration_vectors_rejects_long_mask() {
        let data = [[1.0f32], [2.0], [3.0]];
        let (vectors, _) = calibration_vectors(data.iter(), [true; 4].into_iter());
        vectors.for_each(drop);
    }
}
