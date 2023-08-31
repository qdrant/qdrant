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

/// Validate that `value` is a non-empty string or `None`.
pub fn validate_not_empty(value: &Option<String>) -> Result<(), ValidationError> {
    match value {
        Some(value) if value.is_empty() => Err(ValidationError::new("not_empty")),
        _ => Ok(()),
    }
}

/// Validate the collection name contains no illegal characters.
pub fn validate_collection_name(value: &str) -> Result<(), ValidationError> {
    const INVALID_CHARS: [char; 11] =
        ['<', '>', ':', '"', '/', '\\', '|', '?', '*', '\0', '\u{1F}'];

    match INVALID_CHARS.into_iter().find(|c| value.contains(*c)) {
        Some(c) => {
            let mut err = ValidationError::new("does_not_contain");
            err.add_param(Cow::from("pattern"), &c);
            err.message
                .replace(format!("collection name cannot contain \"{c}\" char").into());
            Err(err)
        }
        None => Ok(()),
    }
}
