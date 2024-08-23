mod search;

use std::fmt::Display;

use segment::types::Filter;

use super::config_diff::StrictModeConfig;
use crate::collection::Collection;

/// Trait to verify strict mode for requests. All functions in this trait will default to an 'empty' implementation,
/// which means every value that needs to be checked must be implemented.
/// This trait ignores the `enabled` parameter in `StrictModeConfig`.
pub trait StrictModeVerification {
    /// Implementing this method allows adding a custom check for request specific values.
    fn check_custom(
        &self,
        _collection: &Collection,
        _strict_mode_config: &StrictModeConfig,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Implement this to check the limit of a request.
    fn request_limit(&self) -> Option<usize> {
        None
    }

    /// Implement this to check the timeout of a request.
    fn request_timeout(&self) -> Option<usize> {
        None
    }

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter operates on a READ-operation, like search.
    /// For filtered updates implement `request_indexed_filter_write`!
    fn request_indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter is used for FILTERED-updates like delete by payload.
    /// For read only filters implement `request_indexed_filter_read`!
    fn request_indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    /// Checks the request limit.
    fn check_request_limit(&self, strict_mode_config: &StrictModeConfig) -> Result<(), String> {
        check_limit_opt(
            self.request_limit(),
            strict_mode_config.max_filter_limit,
            "limit",
        )
    }

    /// Checks the request timeout.
    fn check_request_timeout(&self, strict_mode_config: &StrictModeConfig) -> Result<(), String> {
        check_limit_opt(
            self.request_timeout(),
            strict_mode_config.max_timeout,
            "timeout",
        )
    }

    // Checks all filters use indexed fields only.
    fn check_request_filter(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), String> {
        let check_filter =
            |filter: Option<&Filter>, allow_unindexed_filter: Option<bool>| -> Result<(), String> {
                if let Some(read_filter) = filter {
                    if allow_unindexed_filter == Some(false) {
                        if let Some(key) = collection.filter_without_index(read_filter) {
                            return Err(new_error_msg(
                                format!("Index required but not found for \"{key}\""),
                                "Create an index or use a different filter.",
                            ));
                        }
                    }
                }

                Ok(())
            };

        check_filter(
            self.request_indexed_filter_read(),
            strict_mode_config.unindexed_filtering_retrieve,
        )?;
        check_filter(
            self.request_indexed_filter_write(),
            strict_mode_config.unindexed_filtering_update,
        )?;

        Ok(())
    }

    /// Does the verification of all configured parameters. Only implement this function if you know what
    /// you are doing. In most cases implementing `check_custom` is sufficient.
    fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), String> {
        self.check_custom(collection, strict_mode_config)?;
        self.check_request_limit(strict_mode_config)?;
        self.check_request_filter(collection, strict_mode_config)?;
        Ok(())
    }
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
    check_bool_opt(Some(value), allowed, name, parameter)
}

pub(crate) fn check_bool_opt(
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
