use std::borrow::Cow;
use std::fmt;

use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserializer, Serialize};
use validator::{Validate, ValidationError, ValidationErrors, ValidationErrorsKind};

// Multivector should be small enough to fit the chunk of vector storage

#[cfg(debug_assertions)]
pub const MAX_MULTIVECTOR_FLATTENED_LEN: usize = 32 * 1024;

#[cfg(not(debug_assertions))]
pub const MAX_MULTIVECTOR_FLATTENED_LEN: usize = 1024 * 1024;

/// Validate every item in an iterator and collect per-item errors under a
/// placeholder `?` key. We can't use `ValidationErrors::merge` repeatedly with
/// the same key — its internal `add_nested` panics on the second insert
/// ("Attempt to replace non-empty ValidationErrors entry"). For N≥2 we use a
/// `List` indexed by position; for N=1 we keep the historical `Struct` shape so
/// existing renderings (`?.<field>`) are preserved.
pub fn validate_iter<T: Validate>(iter: impl Iterator<Item = T>) -> Result<(), ValidationErrors> {
    let mut child_errors: Vec<ValidationErrors> = iter.filter_map(|v| v.validate().err()).collect();
    if child_errors.is_empty() {
        return Ok(());
    }

    let kind = if child_errors.len() == 1 {
        ValidationErrorsKind::Struct(Box::new(child_errors.pop().unwrap()))
    } else {
        ValidationErrorsKind::List(
            child_errors
                .into_iter()
                .enumerate()
                .map(|(i, e)| (i, Box::new(e)))
                .collect(),
        )
    };
    let mut bag = ValidationErrors::new();
    bag.errors_mut().insert(Cow::Borrowed("?"), kind);
    Err(bag)
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
    if min.as_ref().is_none_or(|min| &value >= min) && max.as_ref().is_none_or(|max| &value <= max)
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

pub fn deserialize_usize_field<'de, D>(
    deserializer: D,
    field: &str,
    min: usize,
) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let number = deserialize_number(deserializer, field)?;
    number_to_usize(number, field, min)
}

pub fn deserialize_option_usize_field<'de, D>(
    deserializer: D,
    field: &str,
    min: usize,
) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let number = deserialize_optional_number(deserializer, field)?;
    number
        .map(|number| number_to_usize(number, field, min))
        .transpose()
}

pub fn deserialize_u32_field<'de, D>(
    deserializer: D,
    field: &str,
    min: u32,
) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let number = deserialize_number(deserializer, field)?;
    let value = number_to_usize(number, field, min as usize)?;
    u32::try_from(value).map_err(|_| {
        D::Error::custom(format!(
            "{field}: value {value} invalid, must fit into a 32-bit unsigned integer"
        ))
    })
}

pub fn deserialize_option_f32_field<'de, D>(
    deserializer: D,
    field: &str,
) -> Result<Option<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    let number = deserialize_optional_number(deserializer, field)?;
    number
        .map(|number| number_to_f32(number, field))
        .transpose()
}

/// Numeric token captured without materializing or echoing mismatched request data.
#[derive(Clone, Copy)]
enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}

struct NumberVisitor<'a> {
    field: &'a str,
}

impl NumberVisitor<'_> {
    fn invalid_type<E: Error>(&self, actual: &str) -> E {
        E::custom(format!(
            "{}: invalid type {actual}, expected a number",
            self.field
        ))
    }
}

impl<'de> Visitor<'de> for NumberVisitor<'_> {
    type Value = Number;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a number")
    }

    fn visit_u64<E: Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Number::Unsigned(value))
    }

    fn visit_i64<E: Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Number::Signed(value))
    }

    fn visit_f64<E: Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(Number::Float(value))
    }

    fn visit_bool<E: Error>(self, _value: bool) -> Result<Self::Value, E> {
        Err(self.invalid_type("boolean"))
    }

    fn visit_str<E: Error>(self, _value: &str) -> Result<Self::Value, E> {
        Err(self.invalid_type("string"))
    }

    fn visit_bytes<E: Error>(self, _value: &[u8]) -> Result<Self::Value, E> {
        Err(self.invalid_type("byte array"))
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        Err(self.invalid_type("null"))
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        Err(self.invalid_type("null"))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _sequence: A) -> Result<Self::Value, A::Error> {
        Err(self.invalid_type("array"))
    }

    fn visit_map<A: MapAccess<'de>>(self, _map: A) -> Result<Self::Value, A::Error> {
        Err(self.invalid_type("object"))
    }
}

struct OptionalNumberVisitor<'a> {
    field: &'a str,
}

impl<'de> Visitor<'de> for OptionalNumberVisitor<'_> {
    type Value = Option<Number>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a number or null")
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserialize_number(deserializer, self.field).map(Some)
    }
}

fn deserialize_number<'de, D>(deserializer: D, field: &str) -> Result<Number, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(NumberVisitor { field })
}

fn deserialize_optional_number<'de, D>(
    deserializer: D,
    field: &str,
) -> Result<Option<Number>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(OptionalNumberVisitor { field })
}

fn number_to_usize<E>(number: Number, field: &str, min: usize) -> Result<usize, E>
where
    E: Error,
{
    match number {
        Number::Unsigned(value) => usize::try_from(value).map_err(|_| {
            E::custom(format!(
                "{field}: value {value} invalid, must fit into an unsigned integer"
            ))
        }),
        Number::Signed(value) if value >= 0 => usize::try_from(value).map_err(|_| {
            E::custom(format!(
                "{field}: value {value} invalid, must fit into an unsigned integer"
            ))
        }),
        Number::Signed(value) => Err(E::custom(format!(
            "{field}: value {value} invalid, must be {min} or larger"
        ))),
        Number::Float(value) => Err(E::custom(format!(
            "{field}: invalid value {value}, expected an integer"
        ))),
    }
}

fn number_to_f32<E>(number: Number, field: &str) -> Result<f32, E>
where
    E: Error,
{
    let value = match number {
        Number::Unsigned(value) => value as f64,
        Number::Signed(value) => value as f64,
        Number::Float(value) => value,
    };
    if !value.is_finite() || value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
        return Err(E::custom(format!(
            "{field}: value {value} invalid, expected a finite 32-bit float"
        )));
    }
    Ok(value as f32)
}

/// Build the `ValidationError` for a sparse vector configured with the
/// `Turbo4` datatype. Shared between REST and gRPC validators.
pub fn sparse_turbo4_unsupported_error() -> ValidationError {
    let mut err = ValidationError::new("unsupported_sparse_datatype");
    err.message = Some(Cow::Borrowed(
        "sparse vectors do not support the `turbo4` datatype",
    ));
    err
}

/// Validate that `value` is a non-empty string.
pub fn validate_not_empty(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        Err(ValidationError::new("not_empty"))
    } else {
        Ok(())
    }
}

/// Filesystem-unsafe characters rejected for both collection and vector names.
///
/// These end up as path components on disk (collection directories,
/// per-vector storage subdirectories — see
/// `segment_constructor::get_vector_storage_path`), so they must be safe on both
/// Linux and Windows filesystems.
const INVALID_NAME_CHARS: [char; 11] =
    ['<', '>', ':', '"', '/', '\\', '|', '?', '*', '\0', '\u{1F}'];

/// Reject any character from [`INVALID_NAME_CHARS`] in `value`. The `kind`
/// argument is interpolated into the error message ("collection name" /
/// "vector name") so callers get a context-appropriate error.
fn check_invalid_name_chars(value: &str, kind: &str) -> Result<(), ValidationError> {
    let Some(c) = INVALID_NAME_CHARS.into_iter().find(|c| value.contains(*c)) else {
        return Ok(());
    };
    let mut err = ValidationError::new("does_not_contain");
    err.add_param(Cow::from("pattern"), &c);
    err.message
        .replace(format!("{kind} cannot contain \"{c}\" char").into());
    Err(err)
}

/// Validate the collection name contains no illegal characters
///
/// This does not check the length of the name.
pub fn validate_collection_name(value: &str) -> Result<(), ValidationError> {
    check_invalid_name_chars(value, "collection name")
}

/// Validate a named vector identifier.
///
/// Vector names become directory components on disk (see
/// `segment_constructor::get_vector_storage_path`), so they are subject to the same
/// rules as collection names: at most 200 bytes, and free of the
/// filesystem-unsafe characters listed in [`INVALID_NAME_CHARS`].
pub fn validate_vector_name(value: &str) -> Result<(), ValidationError> {
    const MAX_LEN: usize = 200;

    if value.len() > MAX_LEN {
        let mut err = ValidationError::new("length");
        err.add_param(Cow::from("max"), &MAX_LEN);
        err.add_param(Cow::from("actual"), &value.len());
        err.message
            .replace(format!("vector name must be at most {MAX_LEN} bytes long").into());
        return Err(err);
    }

    check_invalid_name_chars(value, "vector name")
}

/// Validate the collection name contains no illegal characters, legacy edition
///
/// Similar to [`validate_collection_name`], but this still allows some special characters that
/// were supported pre Qdrant 1.5. More specifically, this only disallows characters that could
/// never have been used on both Linux and Windows filesystems.
///
/// This does not check the length of the name.
pub fn validate_collection_name_legacy(value: &str) -> Result<(), ValidationError> {
    // Disallowed characters on both Linux/Windows, sourced from: <https://stackoverflow.com/a/31976060/1000145>
    const INVALID_CHARS: [char; 2] = ['/', '\0'];

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
    if multivec_length.contains(&0) {
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

    if !dense_vector_len.is_multiple_of(vectors_count as usize) {
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
        assert!(validate_collection_name("\0").is_err());

        assert!(validate_collection_name_legacy("test_collection").is_ok());
        assert!(validate_collection_name_legacy("").is_ok());
        assert!(validate_collection_name_legacy("no/path").is_err());
        assert!(validate_collection_name_legacy("no*path").is_ok());
        assert!(validate_collection_name_legacy("?").is_ok());
        assert!(validate_collection_name_legacy("\0").is_err());
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
    fn test_validate_iter() {
        #[derive(validator::Validate)]
        struct Item {
            #[validate(range(min = 1))]
            idx: u32,
        }

        // Empty iter — Ok
        assert!(validate_iter(std::iter::empty::<&Item>()).is_ok());

        // All valid — Ok
        let valid = [Item { idx: 1 }, Item { idx: 2 }];
        assert!(validate_iter(valid.iter()).is_ok());

        // Single failure — Struct under `?` (preserves historical `?.<field>`
        // rendering for existing call sites).
        let one_bad = [Item { idx: 0 }];
        let err = validate_iter(one_bad.iter()).expect_err("should fail");
        match err.errors().get("?") {
            Some(ValidationErrorsKind::Struct(_)) => {}
            other => panic!("expected Struct under `?`, got {other:?}"),
        }

        // Two+ failures — must NOT panic (regression: prior impl called
        // `ValidationErrors::merge(_, "?", _)` repeatedly, and validator's
        // internal `add_nested` panics on the second insert).
        let many_bad = [Item { idx: 0 }, Item { idx: 0 }, Item { idx: 0 }];
        let err = validate_iter(many_bad.iter()).expect_err("should fail");
        match err.errors().get("?") {
            Some(ValidationErrorsKind::List(list)) => assert_eq!(list.len(), 3),
            other => panic!("expected List under `?`, got {other:?}"),
        }
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
