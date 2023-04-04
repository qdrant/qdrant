use std::borrow::Cow;
use std::collections::HashMap;

use serde::Serialize;
use validator::{Validate, ValidationError, ValidationErrors};

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

/// Validate that `value` is a non-empty string or `None`.
pub fn validate_not_empty(value: &Option<String>) -> Result<(), ValidationError> {
    match value {
        Some(value) if value.is_empty() => {
            let mut err = ValidationError::new("not_empty");
            err.add_param(Cow::from("value"), &value);
            Err(err)
        }
        _ => Ok(()),
    }
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u64_range_min_1(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1), None)
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u64_range_min_100(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(100), None)
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u64_range_min_1000(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1000), None)
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u32_range_min_1(value: &Option<u32>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(1), None)
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u64_range_min_4_max_10000(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(4), Some(10_000))
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_u64_range_min_4(value: &Option<u64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(4), None)
}

/// Validate that the range of `value` is at least 1 or `None`.
pub fn validate_f64_range_1(value: &Option<f64>) -> Result<(), ValidationError> {
    validate_range_generic(value, Some(0.0), Some(1.0))
}

/// Validate that the range of `value` is at least 1 or `None`.
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
    err.add_param(Cow::from("value"), &value);
    if let Some(min) = min {
        err.add_param(Cow::from("min"), &min);
    }
    if let Some(max) = max {
        err.add_param(Cow::from("max"), &max);
    }
    Err(err)
}
