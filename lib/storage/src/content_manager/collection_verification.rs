use std::iter;
use std::sync::Arc;

use collection::operations::verification::{
    StrictModeVerification, VerificationPass, check_timeout, new_unchecked_verification_pass,
};

use super::errors::StorageError;
use super::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{AccessRequirements, Auth};

/// Checks strict mode using `TableOfContent` instead of `Dispatcher`.
///
/// Note: Avoid this method if you can and use `check_strict_mode_batch` instead to retrieve TOC with the `VerificationPass` gained from the strict mode check.
///       This method should only be used if you only have `TableOfContent` without `Dispatcher`, like in internal API.
pub async fn check_strict_mode_toc_batch<'a, I>(
    requests: impl Iterator<Item = &'a I>,
    timeout: Option<usize>,
    collection_name: &str,
    toc: &TableOfContent,
    auth: &Auth,
) -> Result<VerificationPass, StorageError>
where
    I: StrictModeVerification + 'a,
{
    // Check access here first since strict-mode gets checked before `access`.
    // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
    // information, like existence, strict mode config, payload indices, ...
    //
    // Strict mode have an unlogged access, as actual
    // logging happens in the operations after strict mode check
    let collection_pass = auth
        .unlogged_access() // expected for strict mode check
        .check_collection_access(collection_name, AccessRequirements::new())?;
    let collection = toc.get_collection(&collection_pass).await?;
    if let Some(strict_mode_config) = &collection.strict_mode_config().await
        && strict_mode_config.enabled.unwrap_or_default()
    {
        for request in requests {
            request
                .check_strict_mode(&collection, strict_mode_config)
                .await?;
        }

        if let Some(timeout) = timeout {
            check_timeout(timeout, strict_mode_config)?;
        }
    }

    // It's checked now
    Ok(new_unchecked_verification_pass())
}

pub async fn check_strict_mode_batch<'a, I>(
    requests: impl Iterator<Item = &'a I>,
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    auth: &Auth,
) -> Result<VerificationPass, StorageError>
where
    I: StrictModeVerification + 'a,
{
    let toc = get_toc_without_verification_pass(dispatcher, auth);
    check_strict_mode_toc_batch(requests, timeout, collection_name, toc, auth).await
}

pub async fn check_strict_mode(
    request: &impl StrictModeVerification,
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    auth: &Auth,
) -> Result<VerificationPass, StorageError> {
    check_strict_mode_batch(
        iter::once(request),
        timeout,
        collection_name,
        dispatcher,
        auth,
    )
    .await
}

/// Checks strict mode using `TableOfContent` instead of `Dispatcher`.
///
/// Note: Avoid this method if you can and use `check_strict_mode` instead to retrieve TOC with the `VerificationPass` gained from the strict mode check.
///       This method should only be used if you only have `TableOfContent` without `Dispatcher`, like in internal API.
pub async fn check_strict_mode_toc(
    request: &impl StrictModeVerification,
    timeout: Option<usize>,
    collection_name: &str,
    toc: &TableOfContent,
    auth: &Auth,
) -> Result<VerificationPass, StorageError> {
    check_strict_mode_toc_batch(iter::once(request), timeout, collection_name, toc, auth).await
}

pub async fn check_strict_mode_timeout(
    timeout: Option<usize>,
    collection_name: &str,
    dispatcher: &Dispatcher,
    auth: &Auth,
) -> Result<VerificationPass, StorageError> {
    let Some(timeout) = timeout else {
        return Ok(new_unchecked_verification_pass());
    };

    let toc = get_toc_without_verification_pass(dispatcher, auth);

    // Check access here first since strict-mode gets checked before `access`.
    // If we simply bypassed here, requests to a collection a user doesn't has access to could leak
    // information, like existence, strict mode config, payload indices, ...
    let collection_pass = auth.check_collection_access(
        collection_name,
        AccessRequirements::new(),
        "strict_mode_timeout_check",
    )?;
    let collection = toc.get_collection(&collection_pass).await?;

    if let Some(strict_mode_config) = &collection.strict_mode_config().await
        && strict_mode_config.enabled.unwrap_or_default()
    {
        check_timeout(timeout, strict_mode_config)?;
    }

    // It's checked now
    Ok(new_unchecked_verification_pass())
}

/// Returns the `TableOfContent` from `dispatcher` without needing a validity check.
/// Caution: Do only use this to obtain a `VerificationPass`!
/// Don't make public!
fn get_toc_without_verification_pass<'a>(
    dispatcher: &'a Dispatcher,
    auth: &Auth,
) -> &'a Arc<TableOfContent> {
    let pass = new_unchecked_verification_pass();
    dispatcher.toc(auth, &pass)
}
