use std::borrow::Cow;

use serde::Serialize;
use validator::{Validate, ValidationError, ValidationErrors};

#[allow(clippy::manual_try_fold)] // `try_fold` can't be used because it shortcuts on Err
pub fn validate_iter<T: Validate>(iter: impl Iterator<Item = T>) -> Result<(), ValidationErrors> {
    let errors = iter
        .filter_map(|v| v.validate().err())
        .fold(Err(ValidationErrors::new()), |bag, err| {
            ValidationErrors::merge(bag, "?", Err(err))
        })
        .unwrap_err();
    errors.errors().is_empty().then_some(()).ok_or(errors)
}

#[allow(clippy::manual_try_fold)] // `try_fold` can't be used because it shortcuts on Err
pub fn merge_validation_results(
    results: &[Result<(), ValidationErrors>],
) -> Result<(), ValidationErrors> {
    results
        .iter()
        .filter_map(|result| result.clone().err())
        .fold(Err(ValidationErrors::new()), |bag, err| {
            ValidationErrors::merge(bag, "?", Err(err))
        })
}

/// Validate the value is in `[min, max]`
#[inline]
pub fn validate_range_generic<N>(
    value: N,
    min: Option<N>,
    max: Option<N>,
) -> Result<(), ValidationError>
where
    N: PartialOrd + Serialize,
{
    // If value is within bounds we're good
    if min.as_ref().map(|min| &value >= min).unwrap_or(true)
        && max.as_ref().map(|max| &value <= max).unwrap_or(true)
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

/// Validate the collection name contains no illegal characters
///
/// This does not check the length of the name.
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

/// Validate a polygon has at least 4 points and is closed.
pub fn validate_geo_polygon<T>(points: &Vec<T>) -> Result<(), ValidationError>
where
    T: PartialEq,
{
    let min_length = 4;
    if points.len() < min_length {
        let mut err = ValidationError::new("min_polygon_length");
        err.add_param(Cow::from("length"), &points.len());
        err.add_param(Cow::from("min_length"), &min_length);
        return Err(err);
    }

    let first_point = &points[0];
    let last_point = &points[points.len() - 1];
    if first_point != last_point {
        return Err(ValidationError::new("closed_polygon"));
    }

    Ok(())
}

/// Validate that move shard request has two different peers.
pub fn validate_move_shard_different_peers(
    from_peer_id: u64,
    to_peer_id: u64,
) -> Result<(), ValidationErrors> {
    if to_peer_id != from_peer_id {
        return Ok(());
    }

    let mut errors = ValidationErrors::new();
    errors.add("to_peer_id", {
        let mut error = ValidationError::new("must_not_match");
        error.add_param(Cow::from("value"), &to_peer_id.to_string());
        error.add_param(Cow::from("other_field"), &"from_peer_id");
        error.add_param(Cow::from("other_value"), &from_peer_id.to_string());
        error.add_param(
            Cow::from("message"),
            &format!("cannot move shard to itself, \"to_peer_id\" must be different than {} in \"from_peer_id\"", from_peer_id),
        );
        error
    });
    Err(errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_range_generic() {
        assert!(validate_range_generic(u64::MIN, None, None).is_ok());
        assert!(validate_range_generic(u64::MAX, None, None).is_ok());

        // Min
        assert!(validate_range_generic(1, Some(1), None).is_ok());
        assert!(validate_range_generic(0, Some(1), None).is_err());
        assert!(validate_range_generic(1.0, Some(1.0), None).is_ok());
        assert!(validate_range_generic(0.0, Some(1.0), None).is_err());

        // Max
        assert!(validate_range_generic(1, None, Some(1)).is_ok());
        assert!(validate_range_generic(2, None, Some(1)).is_err());
        assert!(validate_range_generic(1.0, None, Some(1.0)).is_ok());
        assert!(validate_range_generic(2.0, None, Some(1.0)).is_err());

        // Min/max
        assert!(validate_range_generic(0, Some(1), Some(1)).is_err());
        assert!(validate_range_generic(1, Some(1), Some(1)).is_ok());
        assert!(validate_range_generic(2, Some(1), Some(1)).is_err());
        assert!(validate_range_generic(0, Some(1), Some(2)).is_err());
        assert!(validate_range_generic(1, Some(1), Some(2)).is_ok());
        assert!(validate_range_generic(2, Some(1), Some(2)).is_ok());
        assert!(validate_range_generic(3, Some(1), Some(2)).is_err());
        assert!(validate_range_generic(0, Some(2), Some(1)).is_err());
        assert!(validate_range_generic(1, Some(2), Some(1)).is_err());
        assert!(validate_range_generic(2, Some(2), Some(1)).is_err());
        assert!(validate_range_generic(3, Some(2), Some(1)).is_err());
        assert!(validate_range_generic(0.0, Some(1.0), Some(1.0)).is_err());
        assert!(validate_range_generic(1.0, Some(1.0), Some(1.0)).is_ok());
        assert!(validate_range_generic(2.0, Some(1.0), Some(1.0)).is_err());
        assert!(validate_range_generic(0.0, Some(1.0), Some(2.0)).is_err());
        assert!(validate_range_generic(1.0, Some(1.0), Some(2.0)).is_ok());
        assert!(validate_range_generic(2.0, Some(1.0), Some(2.0)).is_ok());
        assert!(validate_range_generic(3.0, Some(1.0), Some(2.0)).is_err());
        assert!(validate_range_generic(0.0, Some(2.0), Some(1.0)).is_err());
        assert!(validate_range_generic(1.0, Some(2.0), Some(1.0)).is_err());
        assert!(validate_range_generic(2.0, Some(2.0), Some(1.0)).is_err());
        assert!(validate_range_generic(3.0, Some(2.0), Some(1.0)).is_err());
    }

    #[test]
    fn test_validate_not_empty() {
        assert!(validate_not_empty(&None).is_ok());
        assert!(validate_not_empty(&Some("not empty".into())).is_ok());
        assert!(validate_not_empty(&Some(" ".into())).is_ok());
        assert!(validate_not_empty(&Some("".into())).is_err());
    }

    #[test]
    fn test_validate_collection_name() {
        assert!(validate_collection_name("test_collection").is_ok());
        assert!(validate_collection_name("").is_ok());
        assert!(validate_collection_name("no/path").is_err());
        assert!(validate_collection_name("no*path").is_err());
        assert!(validate_collection_name("?").is_err());
    }

    #[test]
    fn test_validate_geo_polygon() {
        let bad_polygon: Vec<(f64, f64)> = vec![];
        assert!(
            validate_geo_polygon(&bad_polygon).is_err(),
            "bad polygon should error on validation",
        );

        let bad_polygon = vec![(1., 1.), (2., 2.), (3., 3.)];
        assert!(
            validate_geo_polygon(&bad_polygon).is_err(),
            "bad polygon should error on validation",
        );

        let bad_polygon = vec![(1., 1.), (2., 2.), (3., 3.), (4., 4.)];
        assert!(
            validate_geo_polygon(&bad_polygon).is_err(),
            "bad polygon should error on validation"
        );

        let good_polygon = vec![(1., 1.), (2., 2.), (3., 3.), (1., 1.)];
        assert!(
            validate_geo_polygon(&good_polygon).is_ok(),
            "good polygon should not error on validation",
        );
    }
}
