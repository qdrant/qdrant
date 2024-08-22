use std::sync::Arc;

use collection::collection::Collection;
use collection::operations::config_diff::StrictModeConfig;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::verification::{new_pass, StrictModeVerification, VerificationPass};

use super::errors::StorageError;
use super::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements};

/// Trait for verification checks of collection operation requests.
pub trait CollectionRequestVerification {
    #[allow(async_fn_in_trait)]
    async fn check(
        &self,
        dispatcher: &Dispatcher,
        access: &Access,
        collection_name: &str,
    ) -> Result<VerificationPass, StorageError> {
        let toc = get_toc_without_verification_pass(dispatcher, access);

        self.check_strict_mode(toc, access, collection_name).await?;

        Ok(new_pass())
    }

    #[allow(async_fn_in_trait)]
    async fn check_strict_mode(
        &self,
        toc: &Arc<TableOfContent>,
        access: &Access,
        collection: &str,
    ) -> Result<(), StorageError> {
        // Check access here first since strict-mode gets checked before `access`.
        // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
        // information, like existence, strict mode config, payload indices, ...
        let collection_pass =
            access.check_collection_access(collection, AccessRequirements::new())?;
        let collection = toc.get_collection(&collection_pass).await?;
        let collection_info = collection.info(&ShardSelectorInternal::All).await?;

        if let Some(strict_mode_config) = &collection_info.config.strict_mode_config {
            if strict_mode_config.enabled.unwrap_or_default() {
                self.check_strict_mode_inner(&collection, strict_mode_config)?;
            }
        }

        Ok(())
    }

    fn check_strict_mode_inner(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), StorageError>;
}

/// Returns the `TableOfContents` from `dispatcher` without needing a validity check.
/// Caution: Do only use this to obtain a `VerificationPass`!
/// Don't make public!
fn get_toc_without_verification_pass<'a>(
    dispatcher: &'a Dispatcher,
    access: &Access,
) -> &'a Arc<TableOfContent> {
    let pass = new_pass();
    dispatcher.toc_new(access, &pass)
}

impl<T> CollectionRequestVerification for T
where
    T: StrictModeVerification + Sync,
{
    fn check_strict_mode_inner(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), StorageError> {
        StrictModeVerification::check_strict_mode(self, collection, strict_mode_config)
            .map_err(From::from)
    }
}
