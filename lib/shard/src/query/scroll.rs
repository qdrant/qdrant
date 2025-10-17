use segment::data_types::order_by::OrderBy;
use segment::types::{Filter, WithPayloadInterface, WithVector};

/// Scroll request, used as a part of query request
#[derive(Debug, Clone, PartialEq)]
pub struct QueryScrollRequestInternal {
    /// Page size. Default: 10
    pub limit: usize,

    /// Look only for points which satisfies this conditions. If not provided - all points.
    pub filter: Option<Filter>,

    /// Select which payload to return with the response. Default is true.
    pub with_payload: WithPayloadInterface,

    /// Options for specifying which vectors to include into response. Default is false.
    pub with_vector: WithVector,

    /// Order the records by a payload field.
    pub scroll_order: ScrollOrder,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum ScrollOrder {
    #[default]
    ById,
    ByField(OrderBy),
    Random,
}

pub fn filter_rate_cost(filter: &Filter) -> usize {
    filter.total_conditions_count()
}

/// Base cost for a read operation
pub const BASE_COST: usize = 1;

impl QueryScrollRequestInternal {
    pub fn scroll_rate_cost(&self) -> usize {
        let mut cost = BASE_COST;
        if let Some(filter) = &self.filter {
            cost += filter_rate_cost(filter);
        }
        cost
    }
}
