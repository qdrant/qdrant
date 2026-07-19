use segment::types::Filter;

/// Count request — counts the number of points which match the given conditions.
/// If no filter is provided, the count of all points in the shard is returned.
#[derive(Clone, Debug, PartialEq)]
pub struct CountRequest {
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// If true, count the exact number of points. If false, count the approximate number of
    /// points faster. The approximate count might be unreliable while segments are being
    /// optimized. Default: true.
    pub exact: bool,
}

impl CountRequest {
    pub fn new() -> Self {
        Self {
            filter: None,
            exact: true,
        }
    }
}

impl Default for CountRequest {
    fn default() -> Self {
        Self::new()
    }
}
