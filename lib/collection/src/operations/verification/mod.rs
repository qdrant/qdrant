mod search;

use std::fmt::Display;

use segment::types::Filter;

use super::config_diff::StrictModeConfig;
use super::types::CollectionError;
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

/// Trait to verify strict mode for requests.
/// This trait ignores the `enabled` parameter in `StrictModeConfig`.
pub trait StrictModeVerification {
    /// Implementing this method allows adding a custom check for request specific values.
    fn check_custom(
        &self,
        _collection: &Collection,
        _strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        Ok(())
    }

    /// Implement this to check the limit of a request.
    fn query_limit(&self) -> Option<usize>;

    /// Implement this to check the timeout of a request.
    fn timeout(&self) -> Option<usize>;

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter operates on a READ-operation, like search.
    /// For filtered updates implement `request_indexed_filter_write`!
    fn indexed_filter_read(&self) -> Option<&Filter>;

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter is used for filtered-UPDATES like delete by payload.
    /// For read only filters implement `request_indexed_filter_read`!
    fn indexed_filter_write(&self) -> Option<&Filter>;

    /// Checks the request limit.
    fn check_request_query_limit(
        &self,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_limit_opt(
            self.query_limit(),
            strict_mode_config.max_query_limit,
            "limit",
        )
    }

    /// Checks the request timeout.
    fn check_request_timeout(
        &self,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_limit_opt(self.timeout(), strict_mode_config.max_timeout, "timeout")
    }

    // Checks all filters use indexed fields only.
    fn check_request_filter(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        let check_filter = |filter: Option<&Filter>,
                            allow_unindexed_filter: Option<bool>|
         -> Result<(), CollectionError> {
            if let Some(read_filter) = filter {
                if allow_unindexed_filter == Some(false) {
                    if let Some((key, schemas)) = collection.one_unindexed_key(read_filter) {
                        let possible_schemas_str = schemas
                            .iter()
                            .map(|schema| schema.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");

                        return Err(CollectionError::strict_mode(
                            format!("Index required but not found for \"{key}\" of one of the following types: [{possible_schemas_str}]"),
                            "Create an index for this key or use a different filter.",
                        ));
                    }
                }
            }

            Ok(())
        };

        check_filter(
            self.indexed_filter_read(),
            strict_mode_config.unindexed_filtering_retrieve,
        )?;
        check_filter(
            self.indexed_filter_write(),
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
    ) -> Result<(), CollectionError> {
        self.check_custom(collection, strict_mode_config)?;
        self.check_request_query_limit(strict_mode_config)?;
        self.check_request_filter(collection, strict_mode_config)?;
        Ok(())
    }
}

pub(crate) fn check_bool(
    value: bool,
    allowed: Option<bool>,
    name: &str,
    parameter: &str,
) -> Result<(), CollectionError> {
    check_bool_opt(Some(value), allowed, name, parameter)
}

pub(crate) fn check_bool_opt(
    value: Option<bool>,
    allowed: Option<bool>,
    name: &str,
    parameter: &str,
) -> Result<(), CollectionError> {
    if allowed != Some(false) || !value.unwrap_or_default() {
        return Ok(());
    }

    Err(CollectionError::strict_mode(
        format!("{name} disabled!"),
        format!("Set {parameter}=false."),
    ))
}

pub(crate) fn check_limit_opt<T: PartialOrd + Display>(
    value: Option<T>,
    limit: Option<T>,
    name: &str,
) -> Result<(), CollectionError> {
    let (Some(limit), Some(value)) = (limit, value) else {
        return Ok(());
    };
    if value > limit {
        return Err(CollectionError::strict_mode(
            format!("Limit exceeded {value} > {limit} for \"{name}\""),
            format!("Reduce the \"{name}\" parameter to or below {limit}."),
        ));
    }

    Ok(())
}
