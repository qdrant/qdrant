//! MaxSim of a multi-vector query against stored multi-vectors.
//!
//! A [`MultiQuery`] is a multi-vector query prepared once per search for one
//! storage. It holds the kernel that scores it: one encoded query per inner
//! vector, scored pair by pair, or the same packed for a batched kernel (see
//! [`crate::packed_multi_query`]). Callers score every stored multi-vector the
//! same way, through [`MultiQuery::max_sim`], whichever kernel it holds.

use crate::packed_multi_query::PackedMultiQuery;

/// A multi-vector query prepared for MaxSim against one storage's vectors, made
/// by [`EncodedVectors::encode_multi_query`].
///
/// [`EncodedVectors::encode_multi_query`]: crate::EncodedVectors::encode_multi_query
pub enum MultiQuery<TEncodedQuery> {
    /// One encoded query per inner vector, each scored against every stored vector.
    PerPair(Vec<TEncodedQuery>),
    /// Packed for a batched kernel, which scores all inner vectors at once.
    Packed(PackedMultiQuery),
}

impl<TEncodedQuery> MultiQuery<TEncodedQuery> {
    /// Number of inner vectors.
    pub fn vectors_count(&self) -> usize {
        match self {
            MultiQuery::PerPair(query) => query.len(),
            MultiQuery::Packed(query) => query.vectors_count(),
        }
    }

    /// Start scoring one stored multi-vector: feed its vectors with
    /// [`MaxSim::push`] and take the score from [`MaxSim::finish`].
    ///
    /// `score` scores one encoded query against one stored vector, for
    /// [`MultiQuery::PerPair`].
    pub fn max_sim<F>(&self, score: F) -> MaxSim<'_, TEncodedQuery, F>
    where
        F: Fn(&TEncodedQuery, &[u8]) -> f32,
    {
        let (lanes, scratch) = match self {
            MultiQuery::PerPair(query) => (query.len(), Vec::new()),
            MultiQuery::Packed(query) => (query.lanes(), vec![0; query.scratch_len()]),
        };
        MaxSim {
            query: self,
            score,
            maxima: vec![f32::NEG_INFINITY; lanes],
            scratch,
        }
    }
}

/// Running per-inner-vector maxima of a [`MultiQuery`] against one stored
/// multi-vector.
pub struct MaxSim<'a, TEncodedQuery, F> {
    query: &'a MultiQuery<TEncodedQuery>,
    score: F,
    /// One per inner vector, or per lane of a packed query.
    maxima: Vec<f32>,
    /// Scratch space of a packed kernel.
    scratch: Vec<i16>,
}

impl<TEncodedQuery, F> MaxSim<'_, TEncodedQuery, F>
where
    F: Fn(&TEncodedQuery, &[u8]) -> f32,
{
    /// Fold in one stored vector, as the storage holds it.
    pub fn push(&mut self, vector: &[u8]) {
        match self.query {
            MultiQuery::PerPair(query) => {
                for (max, inner_query) in self.maxima.iter_mut().zip(query) {
                    let sim = (self.score)(inner_query, vector);
                    if *max < sim {
                        *max = sim;
                    }
                }
            }
            MultiQuery::Packed(query) => query.push(&mut self.maxima, &mut self.scratch, vector),
        }
    }

    /// Sum of the per-inner-vector maxima, in inner vector order.
    pub fn finish(self) -> f32 {
        self.maxima[..self.query.vectors_count()].iter().sum()
    }
}
