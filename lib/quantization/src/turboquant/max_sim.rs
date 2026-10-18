//! MaxSim over TurboQuant multivector records, for storages that keep the codes
//! of a [`TurboQuantizer`] directly (the Turbo datatypes) instead of going
//! through [`EncodedVectors`].
//!
//! [`EncodedVectors`]: crate::EncodedVectors

use std::convert::Infallible;

use crate::max_sim::MultiQuery;
use crate::packed_multi_query::PackedMultiQuery;
use crate::turboquant::EncodedQueryTQ;
use crate::turboquant::quantization::TurboQuantizer;

impl TurboQuantizer {
    /// Prepare a multi-vector query, one precomputed query per inner vector, for
    /// [`Self::score_multi_query`]: packed when the batched kernel is faster,
    /// else scored pair by pair with [`Self::score_precomputed`].
    ///
    /// `invert` negates scores, as storages do for distances.
    pub fn encode_multi_query(
        &self,
        query: Vec<EncodedQueryTQ>,
        invert: bool,
    ) -> MultiQuery<EncodedQueryTQ> {
        match self.pack_multi_query_if_faster(&query, invert, false) {
            Some(packed) => MultiQuery::Packed(PackedMultiQuery::TQ(packed)),
            None => MultiQuery::PerPair(query),
        }
    }

    /// MaxSim of a query from [`Self::encode_multi_query`] against one point's
    /// concatenated records. `invert` as there.
    pub fn score_multi_query(
        &self,
        query: &MultiQuery<EncodedQueryTQ>,
        invert: bool,
        records: &[u8],
    ) -> f32 {
        let mut max_sim =
            query.max_sim(|inner, record| signed(invert, self.score_precomputed(inner, record)));
        for record in records.chunks_exact(self.quantized_size()) {
            max_sim.push(record);
        }
        max_sim.finish()
    }

    /// Symmetric MaxSim between two points' concatenated records: for each record
    /// of `a`, its best [`Self::score_symmetric`] against the records of `b`,
    /// summed. The records of `a` are packed when the batched kernel is faster.
    /// `invert` negates scores.
    pub fn score_symmetric_max_sim(&self, records_a: &[u8], records_b: &[u8], invert: bool) -> f32 {
        let size = self.quantized_size();
        if let Some(packed) = self.pack_multi_stored_if_faster(records_a.chunks_exact(size), invert)
        {
            // Packed, so it never scores a pair: no encoded queries to score.
            let query = MultiQuery::<Infallible>::Packed(PackedMultiQuery::TQ(packed));
            let mut max_sim = query.max_sim(|never, _| match *never {});
            for record in records_b.chunks_exact(size) {
                max_sim.push(record);
            }
            return max_sim.finish();
        }

        let mut sum = 0.0;
        for record_a in records_a.chunks_exact(size) {
            let mut max_sim = f32::NEG_INFINITY;
            for record_b in records_b.chunks_exact(size) {
                let sim = signed(invert, self.score_symmetric(record_a, record_b));
                if sim > max_sim {
                    max_sim = sim;
                }
            }
            sum += max_sim;
        }
        sum
    }
}

fn signed(invert: bool, score: f32) -> f32 {
    if invert { -score } else { score }
}
