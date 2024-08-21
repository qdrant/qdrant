mod search;

use std::fmt::Display;

use tokio::sync::RwLockReadGuard;
use tonic::async_trait;

use super::config_diff::StrictModeConfigDiff;
use crate::collection::Collection;

// Creates a new `VerificationPass` for successful verifications.
// Don't use this, unless you know what you're doing!
pub fn new_pass() -> VerificationPass {
    VerificationPass { inner: () }
}

/// A pass, created on successful verification.
pub struct VerificationPass {
    // Private field, so we can't instantiate it from somewhere else.
    #[allow(dead_code)]
    inner: (),
}

#[async_trait]
pub trait StrictModeVerification {
    async fn check_strict_mode(
        &self,
        collection: &RwLockReadGuard<'_, Collection>,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), String>;
}

pub(crate) fn new_error_msg<S>(description: S, solution: &str) -> String
where
    S: ToString,
{
    format!("{}. Help: {solution}", description.to_string())
}

pub(crate) fn check_bool(
    value: bool,
    allowed: Option<bool>,
    name: &str,
    parameter: &str,
) -> Result<(), String> {
    check_bool_op(Some(value), allowed, name, parameter)
}

pub(crate) fn check_bool_op(
    value: Option<bool>,
    allowed: Option<bool>,
    name: &str,
    parameter: &str,
) -> Result<(), String> {
    if allowed != Some(false) || !value.unwrap_or_default() {
        return Ok(());
    }

    Err(new_error_msg(
        format!("{name} disabled!"),
        &format!("Set {parameter}=false."),
    ))
}

pub(crate) fn check_limit_opt<T: PartialOrd + Display>(
    value: Option<T>,
    limit: Option<T>,
    name: &str,
) -> Result<(), String> {
    let Some(limit) = limit else { return Ok(()) };
    let Some(value) = value else { return Ok(()) };
    if value > limit {
        return Err(new_error_msg(
            format!("Limit exceeded {value} > {limit} for \"{name}\""),
            "Reduce the \"{name}\" parameter to or below {limit}.",
        ));
    }

    Ok(())
}

pub(crate) fn check_limit<T: PartialOrd + Display>(
    value: T,
    limit: Option<T>,
    name: &str,
) -> Result<(), String> {
    check_limit_opt(Some(value), limit, name)
}
