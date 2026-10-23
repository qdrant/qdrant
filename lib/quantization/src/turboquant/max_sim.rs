//! MaxSim over TurboQuant multivector records, for storages that keep the codes
//! of a [`TurboQuantizer`] directly (the Turbo datatypes) instead of going
//! through [`EncodedVectors`].
//!
//! [`EncodedVectors`]: crate::EncodedVectors

use crate::max_sim::MultiQuery;
use crate::turboquant::EncodedQueryTQ;
use crate::turboquant::quantization::TurboQuantizer;

impl TurboQuantizer {
    /// MaxSim of a multi-vector query of precomputed queries against one point's
    /// concatenated records. `invert` negates scores, as storages do for distances.
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
    /// summed. `invert` negates scores.
    pub fn score_symmetric_max_sim(&self, records_a: &[u8], records_b: &[u8], invert: bool) -> f32 {
        let size = self.quantized_size();
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
