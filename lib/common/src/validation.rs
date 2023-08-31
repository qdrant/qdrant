use std::borrow::Cow;

use serde::Serialize;
use validator::ValidationError;

/// Validate the value is in `[min, max]`
#[inline]
pub fn validate_range_generic<N>(
    value: &N,
    min: Option<N>,
    max: Option<N>,
) -> Result<(), ValidationError>
where
    N: PartialOrd + Serialize,
{
    // If value is within bounds we're good
    if min.as_ref().map(|min| value >= min).unwrap_or(true)
        && max.as_ref().map(|max| value <= max).unwrap_or(true)
    {
        return Ok(());
    }

    let mut err = ValidationError::new("range");
    if let Some(min) = min {
        err.add_param(Cow::from("min"), &min);
    }
    if let Some(max) = max {
        err.add_param(Cow::from("max"), &max);
    }
    Err(err)
}
