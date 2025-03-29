use std::borrow::Cow;

use serde::Serialize;
use validator::{Validate, ValidationError, ValidationErrors};

// Multivector should be small enough to fit the chunk of vector storage

#[cfg(debug_assertions)]
pub const MAX_MULTIVECTOR_FLATTENED_LEN: usize = 32 * 1024;

#[cfg(not(debug_assertions))]
pub const MAX_MULTIVECTOR_FLATTENED_LEN: usize = 1024 * 1024;

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

/// Validate that `value` is a non-empty string.
pub fn validate_not_empty(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        Err(ValidationError::new("not_empty"))
    } else {
        Ok(())
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
pub fn validate_geo_polygon<T>(points: &[T]) -> Result<(), ValidationError>
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

/// Validate that shard request has two different peers
///
/// We do allow transferring from/to the same peer if the source and target shard are different.
/// This may be used during resharding shard transfers.
pub fn validate_shard_different_peers(
    from_peer_id: u64,
    to_peer_id: u64,
    shard_id: u32,
    to_shard_id: Option<u32>,
) -> Result<(), ValidationErrors> {
    if to_peer_id != from_peer_id {
        return Ok(());
    }

    // If source and target shard is different, we do allow transferring from/to the same peer
    if to_shard_id.is_some_and(|to_shard_id| to_shard_id != shard_id) {
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
            &format!("cannot transfer shard to itself, \"to_peer_id\" must be different than {from_peer_id} in \"from_peer_id\""),
        );
        error
    });
    Err(errors)
}

/// Validate optional lowercase hexadecimal sha256 hash string.
pub fn validate_sha256_hash(value: &str) -> Result<(), ValidationError> {
    if value.len() != 64 {
        let mut err = ValidationError::new("invalid_sha256_hash");
        err.add_param(Cow::from("length"), &value.len());
        err.add_param(Cow::from("expected_length"), &64);
        return Err(err);
    }

    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut err = ValidationError::new("invalid_sha256_hash");
        err.add_param(
            Cow::from("message"),
            &"invalid characters, expected 0-9, a-f, A-F",
        );
        return Err(err);
    }

    Ok(())
}

pub fn validate_multi_vector_by_length(multivec_length: &[usize]) -> Result<(), ValidationErrors> {
    // non_empty
    if multivec_length.is_empty() {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("empty_multi_vector");
        err.add_param(Cow::from("message"), &"multi vector must not be empty");
        errors.add("data", err);
        return Err(errors);
    }

    // check all individual vectors non-empty
    if multivec_length.iter().any(|v| *v == 0) {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("empty_vector");
        err.add_param(Cow::from("message"), &"all vectors must be non-empty");
        errors.add("data", err);
        return Err(errors);
    }

    // total size of all vectors must be less than MAX_MULTIVECTOR_FLATTENED_LEN
    let flattened_len = multivec_length.iter().sum::<usize>();
    if flattened_len >= MAX_MULTIVECTOR_FLATTENED_LEN {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("multi_vector_too_large");
        err.add_param(Cow::from("message"), &format!("Total size of all vectors ({flattened_len}) must be less than {MAX_MULTIVECTOR_FLATTENED_LEN}"));
        errors.add("data", err);
        return Err(errors);
    }

    // all vectors must have the same length
    let dim = multivec_length[0];
    if let Some(bad_vec) = multivec_length.iter().find(|v| **v != dim) {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("inconsistent_multi_vector");
        err.add_param(
            Cow::from("message"),
            &format!(
                "all vectors must have the same dimension, found vector with dimension {bad_vec}",
            ),
        );
        errors.add("data", err);
        return Err(errors);
    }

    Ok(())
}

pub fn validate_multi_vector<T>(multivec: &[Vec<T>]) -> Result<(), ValidationErrors> {
    let multivec_length: Vec<_> = multivec.iter().map(|v| v.len()).collect();
    validate_multi_vector_by_length(&multivec_length)
}

pub fn validate_multi_vector_len(
    vectors_count: u32,
    flatten_dense_vector: &[f32],
) -> Result<(), ValidationErrors> {
    if vectors_count == 0 {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("invalid_vector_count");
        err.add_param(
            Cow::from("vectors_count"),
            &"vectors count must be greater than 0",
        );
        errors.add("data", err);
        return Err(errors);
    }

    let dense_vector_len = flatten_dense_vector.len();
    if dense_vector_len >= MAX_MULTIVECTOR_FLATTENED_LEN {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("Vector size is too large");
        err.add_param(Cow::from("vector_len"), &dense_vector_len);
        err.add_param(Cow::from("vectors_count"), &vectors_count);
        errors.add("data", err);
        return Err(errors);
    }

    if dense_vector_len % vectors_count as usize != 0 {
        let mut errors = ValidationErrors::default();
        let mut err = ValidationError::new("invalid dense vector length for vectors count");
        err.add_param(Cow::from("vector_len"), &dense_vector_len);
        err.add_param(Cow::from("vectors_count"), &vectors_count);
        errors.add("data", err);
        Err(errors)
    } else {
        Ok(())
    }
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
        assert!(validate_not_empty("not empty").is_ok());
        assert!(validate_not_empty(" ").is_ok());
        assert!(validate_not_empty("").is_err());
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

    #[test]
    fn test_validate_sha256_hash() {
        assert!(
            validate_sha256_hash(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            )
            .is_ok(),
        );
        assert!(
            validate_sha256_hash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde")
                .is_err(),
        );
        assert!(
            validate_sha256_hash(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"
            )
            .is_err(),
        );
        assert!(
            validate_sha256_hash(
                "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEG"
            )
            .is_err(),
        );
    }
}
