//! Batched MaxSim over quantized multi-vectors.
//!
//! Scoring a multi-vector query token by token runs the single-vector kernel
//! once per `query x document` token pair: one dispatch, one horizontal sum and
//! one score postprocess per pair, and nothing reused between pairs even though
//! the query is fixed for the whole search. A [`PackedMultiQuery`] lays the query
//! out once so that SIMD lanes hold query tokens; each stored vector of a
//! document is then scored against all query tokens at once and folded into
//! their running maxima with [`MaxSim::push`].
//!
//! Every kind repeats the single-vector arithmetic in the same order, so the
//! scores are the same as scoring pair by pair.

mod binary;
pub mod crossover;
mod pq;
mod u8;

pub use binary::PackedMultiQueryBinary;
pub use pq::PackedMultiQueryPQ;
pub use u8::PackedMultiQueryU8;

pub use crate::turboquant::packed::PackedMultiQueryTQ;

/// Query tokens per packed block: two AVX registers of 8 lanes.
pub(crate) const BLOCK: usize = 16;

/// A multi-vector query packed for batched MaxSim against one quantized storage.
pub enum PackedMultiQuery {
    U8(PackedMultiQueryU8),
    TQ(PackedMultiQueryTQ),
    Binary(PackedMultiQueryBinary),
    PQ(PackedMultiQueryPQ),
}

impl PackedMultiQuery {
    pub fn vectors_count(&self) -> usize {
        match self {
            PackedMultiQuery::U8(query) => query.vectors_count(),
            PackedMultiQuery::TQ(query) => query.vectors_count(),
            PackedMultiQuery::Binary(query) => query.vectors_count(),
            PackedMultiQuery::PQ(query) => query.vectors_count(),
        }
    }

    /// Packed, but on a kernel slower than scoring pair by pair: TurboQuant
    /// accumulating in `i64`.
    pub fn is_slow(&self) -> bool {
        match self {
            PackedMultiQuery::TQ(query) => query.is_wide(),
            PackedMultiQuery::U8(_) | PackedMultiQuery::Binary(_) | PackedMultiQuery::PQ(_) => {
                false
            }
        }
    }

    fn lanes(&self) -> usize {
        match self {
            PackedMultiQuery::U8(query) => query.lanes(),
            PackedMultiQuery::TQ(query) => query.lanes(),
            PackedMultiQuery::Binary(query) => query.lanes(),
            PackedMultiQuery::PQ(query) => query.lanes(),
        }
    }

    /// Start scoring multi-vectors: feed the stored vectors of one with
    /// [`MaxSim::push`], take its score from [`MaxSim::finish`], and go on with
    /// the next one.
    pub fn max_sim(&self) -> MaxSim<'_> {
        let scratch = match self {
            PackedMultiQuery::TQ(query) => vec![0; query.scratch_len()],
            PackedMultiQuery::U8(_) | PackedMultiQuery::Binary(_) | PackedMultiQuery::PQ(_) => {
                Vec::new()
            }
        };
        MaxSim {
            query: self,
            maxima: vec![f32::NEG_INFINITY; self.lanes()],
            scratch,
        }
    }
}

/// Running per-query-token maxima of one multi-vector at a time.
pub struct MaxSim<'a> {
    query: &'a PackedMultiQuery,
    maxima: Vec<f32>,
    /// Decoded stored vector, for TurboQuant.
    scratch: Vec<i16>,
}

impl MaxSim<'_> {
    /// Fold in one stored vector, as the storage holds it.
    pub fn push(&mut self, encoded_vector: &[u8]) {
        match self.query {
            PackedMultiQuery::U8(query) => query.push(&mut self.maxima, encoded_vector),
            PackedMultiQuery::TQ(query) => {
                query.push(&mut self.maxima, &mut self.scratch, encoded_vector)
            }
            PackedMultiQuery::Binary(query) => query.push(&mut self.maxima, encoded_vector),
            PackedMultiQuery::PQ(query) => query.push(&mut self.maxima, encoded_vector),
        }
    }

    /// Sum of the per-query-token maxima, in query token order, and reset for
    /// the next multi-vector.
    pub fn finish(&mut self) -> f32 {
        let score = self.maxima[..self.query.vectors_count()].iter().sum();
        self.maxima.fill(f32::NEG_INFINITY);
        score
    }
}
