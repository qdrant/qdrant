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

/// A multi-vector query packed for batched MaxSim against one quantized storage.
///
/// No storage has a batched kernel yet, so there is no packed query: every
/// [`MultiQuery`] scores pair by pair.
///
/// [`MultiQuery`]: crate::max_sim::MultiQuery
pub enum PackedMultiQuery {}

impl PackedMultiQuery {
    pub fn vectors_count(&self) -> usize {
        match *self {}
    }

    /// Packed, but on a kernel slower than scoring pair by pair.
    pub fn is_slow(&self) -> bool {
        match *self {}
    }

    /// Running maxima a [`MaxSim`] keeps for this query: one per lane.
    ///
    /// [`MaxSim`]: crate::max_sim::MaxSim
    pub(crate) fn lanes(&self) -> usize {
        match *self {}
    }

    /// Scratch space [`Self::push`] needs.
    pub(crate) fn scratch_len(&self) -> usize {
        match *self {}
    }

    /// Fold one stored vector, as the storage holds it, into the per-lane maxima.
    pub(crate) fn push(&self, _maxima: &mut [f32], _scratch: &mut [i16], _encoded_vector: &[u8]) {
        match *self {}
    }
}
