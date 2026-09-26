use std::collections::{BTreeSet, HashMap};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::OperationResult;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::entry::ReadSegmentEntry;
use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};
use shard::count::CountRequestInternal;
use shard::facet::FacetRequestInternal;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// Returns facet hits for the given facet request.
    ///
    /// Counts the number of points for each unique value of the specified payload key,
    /// optionally filtering by the given conditions.
    pub(crate) fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse> {
        self.check_stopped()?;
        let FacetRequestInternal {
            key,
            limit,
            filter,
            exact,
        } = request;

        if exact {
            return self.exact_facet(key, limit, filter);
        }

        let hw_acc = HwMeasurementAcc::disposable_edge();

        let facet_params = FacetParams {
            key,
            limit,
            filter,
            exact,
        };

        // Facet every segment in parallel, then merge the per-segment counts sequentially.
        let per_segment = self.par_map_segments(|segment| {
            segment.read_segment().facet(
                &facet_params,
                &self.is_stopped,
                &hw_acc.get_counter_cell(),
            )
        })?;

        let mut merged_counts = HashMap::new();
        for segment_result in per_segment {
            self.check_stopped()?;
            for (value, count) in segment_result {
                *merged_counts.entry(value).or_insert(0) += count;
            }
        }

        Ok(FacetResponse::top_hits(merged_counts, limit))
    }

    /// Counts every unique value with an exact count, so a point stored in several segments
    /// (in different versions) is counted once.
    fn exact_facet(
        &self,
        key: JsonPath,
        limit: usize,
        filter: Option<Filter>,
    ) -> OperationResult<FacetResponse> {
        let per_segment = self.par_map_segments(|segment| {
            segment.read_segment().unique_values(
                &key,
                filter.as_ref(),
                &self.is_stopped,
                &HardwareCounterCell::disposable(),
            )
        })?;
        let unique_values: BTreeSet<_> = per_segment.into_iter().flatten().collect();

        let mut counts = HashMap::with_capacity(unique_values.len());
        for value in unique_values {
            self.check_stopped()?;
            let match_value = Filter::new_must(Condition::Field(FieldCondition::new_match(
                key.clone(),
                Match::new_value(ValueVariants::from(value.clone())),
            )));
            let count = self.count(CountRequestInternal {
                filter: Filter::merge_opts(filter.clone(), Some(match_value)),
                exact: true,
            })?;
            counts.insert(value, count);
        }

        Ok(FacetResponse::top_hits(counts, limit))
    }
}

#[cfg(test)]
mod tests {
    use ahash::AHashSet;
    use segment::data_types::facets::{FacetValue, FacetValueHit};
    use segment::types::{HasIdCondition, PayloadFieldSchema, PayloadSchemaType};
    use shard::operations::CollectionUpdateOperations::FieldIndexOperation;
    use shard::operations::{CreateIndex, FieldIndexOperations};

    use super::*;
    use crate::test_helpers::{point_with_group, test_config, upsert};
    use crate::{EdgeShard, FacetRequest};

    #[test]
    fn exact_facet_counts_filtered_values() {
        let dir = tempfile::tempdir().unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        shard
            .update(FieldIndexOperation(FieldIndexOperations::CreateIndex(
                CreateIndex {
                    field_name: "group".parse().unwrap(),
                    field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
                },
            )))
            .unwrap();
        upsert(
            &shard,
            vec![
                point_with_group(1, "a"),
                point_with_group(2, "a"),
                point_with_group(3, "a"),
                point_with_group(4, "b"),
                point_with_group(5, "b"),
                point_with_group(6, "c"),
            ],
        );

        let only_ids = |ids: &[u64]| {
            Filter::new_must(Condition::HasId(HasIdCondition::from(
                ids.iter().map(|&id| id.into()).collect::<AHashSet<_>>(),
            )))
        };
        let facet = |limit: usize, filter: Option<Filter>| {
            let mut request = FacetRequest::new("group".parse().unwrap());
            request.exact = true;
            request.limit = limit;
            request.filter = filter;
            shard.facet(request).unwrap().hits
        };
        let hit = |value: &str, count: usize| FacetValueHit {
            value: FacetValue::Keyword(value.to_string()),
            count,
        };

        assert_eq!(facet(2, None), vec![hit("a", 3), hit("b", 2)]);
        assert_eq!(
            facet(10, Some(only_ids(&[1, 4, 5, 6]))),
            vec![hit("b", 2), hit("a", 1), hit("c", 1)],
        );
    }
}
