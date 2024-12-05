use std::iter;
use std::sync::Arc;

use collection::operations::verification::{
    check_timeout, new_unchecked_verification_pass, StrictModeVerification, VerificationPass,
};

use super::errors::StorageError;
use super::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements};

pub async fn check_strict_mode_batch<'a, I>(
    requests: impl Iterator<Item = &'a I>,
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    access: &Access,
) -> Result<VerificationPass, StorageError>
where
    I: StrictModeVerification + 'a,
{
    let toc = get_toc_without_verification_pass(dispatcher, access);

    // Check access here first since strict-mode gets checked before `access`.
    // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
    // information, like existence, strict mode config, payload indices, ...
    let collection_pass =
        access.check_collection_access(collection_name, AccessRequirements::new())?;
    let collection = toc.get_collection(&collection_pass).await?;
    if let Some(strict_mode_config) = &collection.strict_mode_config().await {
        if strict_mode_config.enabled.unwrap_or_default() {
            for request in requests {
                request
                    .check_strict_mode(&collection, strict_mode_config)
                    .await?;
            }

            if let Some(timeout) = timeout {
                check_timeout(timeout, strict_mode_config)?;
            }
        }
    }

    // It's checked now
    Ok(new_unchecked_verification_pass())
}

pub async fn check_strict_mode(
    request: &impl StrictModeVerification,
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    access: &Access,
) -> Result<VerificationPass, StorageError> {
    check_strict_mode_batch(
        iter::once(request),
        timeout,
        collection_name,
        dispatcher,
        access,
    )
    .await
}

pub async fn check_strict_mode_timeout(
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    access: &Access,
) -> Result<VerificationPass, StorageError> {
    let Some(timeout) = timeout else {
        return Ok(new_unchecked_verification_pass());
    };

    let toc = get_toc_without_verification_pass(dispatcher, access);

    // Check access here first since strict-mode gets checked before `access`.
    // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
    // information, like existence, strict mode config, payload indices, ...
    let collection_pass =
        access.check_collection_access(collection_name, AccessRequirements::new())?;
    let collection = toc.get_collection(&collection_pass).await?;

    if let Some(strict_mode_config) = &collection.strict_mode_config().await {
        if strict_mode_config.enabled.unwrap_or_default() {
            check_timeout(timeout, strict_mode_config)?;
        }
    }

    // It's checked now
    Ok(new_unchecked_verification_pass())
}

/// Returns the `TableOfContents` from `dispatcher` without needing a validity check.
/// Caution: Do only use this to obtain a `VerificationPass`!
/// Don't make public!
fn get_toc_without_verification_pass<'a>(
    dispatcher: &'a Dispatcher,
    access: &Access,
) -> &'a Arc<TableOfContent> {
    let pass = new_unchecked_verification_pass();
    dispatcher.toc(access, &pass)
}
