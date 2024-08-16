use std::sync::Arc;

use collection::operations::config_diff::StrictModeConfigDiff;
use collection::operations::types::SearchRequest;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tonic::async_trait;

use crate::common::collections::do_get_collection;

/// Trait for verification checks of collection operation requests.
#[async_trait]
pub trait CollectionRequestVerification {
    async fn check(
        &self,
        dispatcher: &Dispatcher,
        access: &Access,
        collection_name: &str,
    ) -> Result<VerificationPass, StorageError> {
        let toc = get_toc_unchecked(dispatcher, access);

        self.check_strict_mode(toc, access, collection_name).await?;

        Ok(new_pass())
    }

    async fn check_strict_mode(
        &self,
        toc: &Arc<TableOfContent>,
        access: &Access,
        collection: &str,
    ) -> Result<(), StorageError> {
        let collection_info = do_get_collection(toc, access.clone(), collection, None).await?;

        if let Some(strict_mode_config) = &collection_info.config.strict_mode_config {
            self.check_strict_mode_inner(strict_mode_config).await?;
        }

        Ok(())
    }

    async fn check_strict_mode_inner(
        &self,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), StorageError>;
}

/// Returns the `TableOfContents` from `dispatcher` without needing a validity check.
/// Caution: Do only use this to obtain a `VerificationPass`!
/// Don't make public!
fn get_toc_unchecked<'a>(dispatcher: &'a Dispatcher, access: &Access) -> &'a Arc<TableOfContent> {
    let _pass = new_pass();
    // TODO: pass '_pass' to `toc`.
    dispatcher.toc(access)
}

// Creates a new `VerificationPass` for successful verifications.
// TODO: Make private
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
impl CollectionRequestVerification for SearchRequest {
    async fn check_strict_mode_inner(
        &self,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), StorageError> {
        if let Some(filter_limit) = strict_mode_config.max_filter_limit {
            if self.search_request.filter.is_some() {
                let limit = self.search_request.limit;
                if filter_limit > limit {
                    return Err(new_error(
                        format!("Max search filter limit of {filter_limit} exceeded"),
                        "Reduce the filter limit",
                    ));
                }
            }
        }

        Ok(())
    }
}

fn new_error<S>(description: S, solution: &str) -> StorageError
where
    S: ToString,
{
    let description = format!("{}. Help: {solution}", description.to_string());
    StorageError::forbidden(description)
}
