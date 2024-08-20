use std::sync::Arc;

use collection::collection::Collection;
use collection::operations::config_diff::StrictModeConfigDiff;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::verification::{new_pass, StrictModeVerification, VerificationPass};
use tokio::sync::RwLockReadGuard;
use tonic::async_trait;

use super::errors::StorageError;
use super::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements};

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
        let collection_pass =
            access.check_collection_access(collection, AccessRequirements::new().whole())?;
        let collection = toc.get_collection(&collection_pass).await?;
        let collection_info = collection.info(&ShardSelectorInternal::All).await?;

        if let Some(strict_mode_config) = &collection_info.config.strict_mode_config {
            if strict_mode_config.enabled == Some(true) {
                self.check_strict_mode_inner(&collection, strict_mode_config)
                    .await?;
            }
        }

        Ok(())
    }

    async fn check_strict_mode_inner(
        &self,
        collection: &RwLockReadGuard<'_, Collection>,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), StorageError>;
}

/// Returns the `TableOfContents` from `dispatcher` without needing a validity check.
/// Caution: Do only use this to obtain a `VerificationPass`!
/// Don't make public!
fn get_toc_unchecked<'a>(dispatcher: &'a Dispatcher, access: &Access) -> &'a Arc<TableOfContent> {
    let pass = new_pass();
    dispatcher.toc_new(access, &pass)
}

#[async_trait]
impl<T> CollectionRequestVerification for T
where
    T: StrictModeVerification + Sync,
{
    async fn check_strict_mode_inner(
        &self,
        collection: &RwLockReadGuard<'_, Collection>,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), StorageError> {
        StrictModeVerification::check_strict_mode(self, collection, strict_mode_config)
            .await
            .map_err(StorageError::forbidden)
    }
}
