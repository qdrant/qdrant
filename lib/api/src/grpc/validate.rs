use std::borrow::Cow;
use std::collections::HashMap;

use serde::Serialize;
use validator::{Validate, ValidationError, ValidationErrors};

use super::qdrant::{GeoPoint, NamedVectors};

pub trait ValidateExt {
    fn validate(&self) -> Result<(), ValidationErrors>;
}

impl Validate for dyn ValidateExt {
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        ValidateExt::validate(self)
    }
}

impl<V> ValidateExt for ::core::option::Option<V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        (&self).validate()
    }
}

impl<V> ValidateExt for &::core::option::Option<V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        self.as_ref().map(Validate::validate).unwrap_or(Ok(()))
    }
}

impl<V> ValidateExt for Vec<V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        let errors = self
            .iter()
            .filter_map(|v| v.validate().err())
            .fold(Err(ValidationErrors::new()), |bag, err| {
                ValidationErrors::merge(bag, "?", Err(err))
            })
            .unwrap_err();
        errors.errors().is_empty().then_some(()).ok_or(errors)
    }
}

impl<K, V> ValidateExt for HashMap<K, V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        let errors = self
            .values()
            .filter_map(|v| v.validate().err())
            .fold(Err(ValidationErrors::new()), |bag, err| {
                ValidationErrors::merge(bag, "?", Err(err))
            })
            .unwrap_err();
        errors.errors().is_empty().then_some(()).ok_or(errors)
    }
}

impl Validate for crate::grpc::qdrant::vectors_config::Config {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use crate::grpc::qdrant::vectors_config::Config;
        match self {
            Config::Params(params) => params.validate(),
            Config::ParamsMap(params_map) => params_map.validate(),
        }
    }
}

impl Validate for crate::grpc::qdrant::update_vectors_config::Config {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use crate::grpc::qdrant::update_vectors_config::Config;
        match self {
            Config::Params(params) => params.validate(),
            Config::ParamsMap(params_map) => params_map.validate(),
        }
    }
}

impl Validate for crate::grpc::qdrant::quantization_config::Quantization {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use crate::grpc::qdrant::quantization_config::Quantization;
        match self {
            Quantization::Scalar(scalar) => scalar.validate(),
            Quantization::Product(product) => product.validate(),
        }
    }
}

/// Validate that `value` is a non-empty string or `None`.
pub fn validate_not_empty(value: &Option<String>) -> Result<(), ValidationError> {
    match value {
        Some(value) if value.is_empty() => Err(ValidationError::new("not_empty")),
        _ => Ok(()),
    }
}

/// Validate the value is in `[1, ]` or `None`.
pub fn validate_u64_range_min_1(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1), None)
}

/// Validate the value is in `[1, ]` or `None`.
pub fn validate_u32_range_min_1(value: &Option<u32>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1), None)
}

/// Validate the value is in `[100, ]` or `None`.
pub fn validate_u64_range_min_100(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(100), None)
}

/// Validate the value is in `[1000, ]` or `None`.
pub fn validate_u64_range_min_1000(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1000), None)
}

/// Validate the value is in `[4, ]` or `None`.
pub fn validate_u64_range_min_4(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(4), None)
}

/// Validate the value is in `[4, 10000]` or `None`.
pub fn validate_u64_range_min_4_max_10000(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(4), Some(10_000))
}

/// Validate the value is in `[0.5, 1.0]` or `None`.
pub fn validate_f32_range_min_0_5_max_1(value: &Option<f32>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(0.5), Some(1.0))
}

/// Validate the value is in `[0.0, 1.0]` or `None`.
pub fn validate_f64_range_1(value: &Option<f64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(0.0), Some(1.0))
}

/// Validate the value is in `[1.0, ]` or `None`.
pub fn validate_f64_range_min_1(value: &Option<f64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1.0), None)
}

/// Validate the value is in `[min, max]` or `None`.
#[inline]
pub fn validate_range_generic<N>(
    value: &Option<N>,
    min: Option<N>,
    max: Option<N>,
) -> Result<(), ValidationError>
where
    N: PartialOrd + Serialize,
{
    // If value is None we're good
    let value = match value {
        Some(value) => value,
        None => return Ok(()),
    };

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

/// Validate the list of named vectors is not empty.
pub fn validate_named_vectors_not_empty(
    value: &Option<NamedVectors>,
) -> Result<(), ValidationError> {
    // If length is non-zero, we're good
    match value {
        Some(vectors) if !vectors.vectors.is_empty() => return Ok(()),
        Some(_) | None => {}
    }

    let mut err = ValidationError::new("length");
    err.add_param(Cow::from("min"), &1);
    Err(err)
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

/// Validate a polygon has at least 4 points and is closed.
pub fn validate_geo_polygon(points: &Vec<GeoPoint>) -> Result<(), ValidationError> {
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

#[cfg(test)]
mod tests {
    use validator::Validate;

    use crate::grpc::qdrant::{
        CreateCollection, CreateFieldIndexCollection, GeoPoint, GeoPolygon, SearchPoints,
        UpdateCollection,
    };

    #[test]
    fn test_good_request() {
        let bad_request = CreateCollection {
            collection_name: "test_collection".into(),
            timeout: Some(10),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );

        // Collection name validation must not be strict on non-creation
        let bad_request = UpdateCollection {
            collection_name: "no/path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );

        // Collection name validation must not be strict on non-creation
        let bad_request = UpdateCollection {
            collection_name: "no*path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );
    }

    #[test]
    fn test_bad_collection_request() {
        let bad_request = CreateCollection {
            collection_name: "".into(),
            timeout: Some(0),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );

        // Collection name validation must be strict on creation
        let bad_request = CreateCollection {
            collection_name: "no/path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );

        // Collection name validation must be strict on creation
        let bad_request = CreateCollection {
            collection_name: "no*path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );
    }

    #[test]
    fn test_bad_index_request() {
        let bad_request = CreateFieldIndexCollection {
            collection_name: "".into(),
            field_name: "".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad index request should error on validation"
        );
    }

    #[test]
    fn test_bad_search_request() {
        let bad_request = SearchPoints {
            collection_name: "".into(),
            limit: 0,
            vector_name: Some("".into()),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );

        let bad_request = SearchPoints {
            limit: 0,
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );

        let bad_request = SearchPoints {
            vector_name: Some("".into()),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );
    }

    #[test]
    fn test_geo_polygon() {
        let bad_polygon = GeoPolygon { points: vec![] };
        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let bad_polygon = GeoPolygon {
            points: vec![
                GeoPoint { lat: 1., lon: 1. },
                GeoPoint { lat: 2., lon: 2. },
                GeoPoint { lat: 3., lon: 3. },
            ],
        };
        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let bad_polygon = GeoPolygon {
            points: vec![
                GeoPoint { lat: 1., lon: 1. },
                GeoPoint { lat: 2., lon: 2. },
                GeoPoint { lat: 3., lon: 3. },
                GeoPoint { lat: 4., lon: 4. },
            ],
        };

        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let good_polygon = GeoPolygon {
            points: vec![
                GeoPoint { lat: 1., lon: 1. },
                GeoPoint { lat: 2., lon: 2. },
                GeoPoint { lat: 3., lon: 3. },
                GeoPoint { lat: 1., lon: 1. },
            ],
        };
        assert!(
            good_polygon.validate().is_ok(),
            "good polygon should not error on validation"
        );
    }
}
