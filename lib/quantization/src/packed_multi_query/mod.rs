//! Batched MaxSim over quantized multi-vectors.
//!
//! Scoring a multi-vector query token by token runs the single-vector kernel
//! once per `query x document` token pair: one dispatch, one horizontal sum and
//! one score postprocess per pair, and nothing reused between pairs even though
//! the query is fixed for the whole search. A [`PackedMultiQuery`] lays the query
//! out once so that SIMD lanes hold query tokens; each stored vector of a
//! document is then scored against all query tokens at once and folded into
//! their running maxima. It is one of the kernels a [`MultiQuery`] holds.
//!
//! Every kind repeats the single-vector arithmetic in the same order, so the
//! scores are the same as scoring pair by pair.
//!
//! [`MultiQuery`]: crate::max_sim::MultiQuery

pub mod crossover;
mod u8;

pub use u8::PackedMultiQueryU8;

pub use crate::turboquant::packed::PackedMultiQueryTQ;

/// Query tokens per packed block: two AVX registers of 8 lanes.
pub(crate) const BLOCK: usize = 16;

/// A multi-vector query packed for batched MaxSim against one quantized storage.
pub enum PackedMultiQuery {
    U8(PackedMultiQueryU8),
    TQ(PackedMultiQueryTQ),
}

impl PackedMultiQuery {
    pub fn vectors_count(&self) -> usize {
        match self {
            PackedMultiQuery::U8(query) => query.vectors_count(),
            PackedMultiQuery::TQ(query) => query.vectors_count(),
        }
    }

    /// Packed, but on a kernel slower than scoring pair by pair: TurboQuant
    /// accumulating in `i64`.
    pub fn is_slow(&self) -> bool {
        match self {
            PackedMultiQuery::TQ(query) => query.is_wide(),
            PackedMultiQuery::U8(_) => false,
        }
    }

    /// Running maxima a [`MaxSim`] keeps for this query: one per lane.
    ///
    /// [`MaxSim`]: crate::max_sim::MaxSim
    pub(crate) fn lanes(&self) -> usize {
        match self {
            PackedMultiQuery::U8(query) => query.lanes(),
            PackedMultiQuery::TQ(query) => query.lanes(),
        }
    }

    /// Scratch space [`Self::push`] needs: a decoded stored vector, for TurboQuant.
    pub(crate) fn scratch_len(&self) -> usize {
        match self {
            PackedMultiQuery::TQ(query) => query.scratch_len(),
            PackedMultiQuery::U8(_) => 0,
        }
    }

    /// Fold one stored vector, as the storage holds it, into the per-lane maxima.
    pub(crate) fn push(&self, maxima: &mut [f32], scratch: &mut [i16], encoded_vector: &[u8]) {
        match self {
            PackedMultiQuery::U8(query) => query.push(maxima, encoded_vector),
            PackedMultiQuery::TQ(query) => query.push(maxima, scratch, encoded_vector),
        }
    }
}
