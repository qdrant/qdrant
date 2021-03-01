use crate::index::index::{QueryEstimator, PayloadIndex};
use crate::types::{Filter, TheMap, PayloadKeyType, PayloadType, Condition};
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::estimations::estimate_filter;
use crate::index::struct_payload_index::StructPayloadIndex;

impl QueryEstimator for StructPayloadIndex {
    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation {

        let total = self.total_points();

        let estimator = |condition: &Condition| {

            CardinalityEstimation {
                primary_clauses: vec![],
                min: 0,
                exp: 0,
                max: 0
            }
        };

        estimate_filter(&estimator, query, total)
    }
}