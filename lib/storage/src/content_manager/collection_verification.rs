use std::sync::Arc;

use collection::operations::verification::{new_pass, StrictModeVerification, VerificationPass};

use super::errors::StorageError;
use super::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements};

pub async fn check_strict_mode(
    request: &impl StrictModeVerification,
    collection_name: &str,
    dispatcher: &Dispatcher,
    access: &Access,
) -> Result<VerificationPass, StorageError> {
    let toc = get_toc_without_verification_pass(dispatcher, access);

    // Check access here first since strict-mode gets checked before `access`.
    // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
    // information, like existence, strict mode config, payload indices, ...
    let collection_pass =
        access.check_collection_access(collection_name, AccessRequirements::new())?;
    let collection = toc.get_collection(&collection_pass).await?;
    if let Some(strict_mode_config) = &collection.strict_mode_config().await {
        if strict_mode_config.enabled.unwrap_or_default() {
            request.check_strict_mode(&collection, strict_mode_config)?;
        }
    }

    Ok(new_pass())
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
