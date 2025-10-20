use segment::types::Filter;

/// Base cost for a read operation
pub const BASE_COST: usize = 1;

pub fn filter_rate_cost(filter: &Filter) -> usize {
    filter.total_conditions_count()
}
