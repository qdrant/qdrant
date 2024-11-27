use std::sync::Arc;

use collection::operations::verification::StrictModeVerification;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tonic::Status;

/// Trait for different ways of providing something with `toc` that may do additional checks eg. for Strict mode.
pub trait CheckedTocProvider {
    async fn check_strict_mode<'b>(
        &'b self,
        request: &impl StrictModeVerification,
        collection_name: &str,
        timeout: Option<usize>,
        access: &Access,
    ) -> Result<&'b Arc<TableOfContent>, Status>;

    async fn check_strict_mode_batch<'b, I, R>(
        &'b self,
        requests: &[I],
        conv: impl Fn(&I) -> &R,
        collection_name: &str,
        timeout: Option<usize>,
        access: &Access,
    ) -> Result<&'b Arc<TableOfContent>, Status>
    where
        R: StrictModeVerification;
}

/// Simple provider for TableOfContent that doesn't do any checks.
pub struct UncheckedTocProvider<'a> {
    toc: &'a Arc<TableOfContent>,
}

impl<'a> UncheckedTocProvider<'a> {
    pub fn new_unchecked(toc: &'a Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

impl CheckedTocProvider for UncheckedTocProvider<'_> {
    async fn check_strict_mode<'b>(
        &'b self,
        _request: &impl StrictModeVerification,
        _collection_name: &str,
        _timeout: Option<usize>,
        _access: &Access,
    ) -> Result<&'b Arc<TableOfContent>, Status> {
        // No checks here
        Ok(self.toc)
    }

    async fn check_strict_mode_batch<'b, I, R>(
        &'b self,
        _requests: &[I],
        _conv: impl Fn(&I) -> &R,
        _collection_name: &str,
        _timeout: Option<usize>,
        _access: &Access,
    ) -> Result<&'b Arc<TableOfContent>, Status>
    where
        R: StrictModeVerification,
    {
        // No checks here
        Ok(self.toc)
    }
}

/// Provider for TableOfContent that requires Strict mode to be checked.
pub struct StrictModeCheckedTocProvider<'a> {
    dispatcher: &'a Dispatcher,
}

impl<'a> StrictModeCheckedTocProvider<'a> {
    pub fn new(dispatcher: &'a Dispatcher) -> Self {
        Self { dispatcher }
    }
}

impl CheckedTocProvider for StrictModeCheckedTocProvider<'_> {
    async fn check_strict_mode(
        &self,
        request: &impl StrictModeVerification,
        collection_name: &str,
        timeout: Option<usize>,
        access: &Access,
    ) -> Result<&Arc<TableOfContent>, Status> {
        let pass =
            check_strict_mode(request, timeout, collection_name, self.dispatcher, access).await?;
        Ok(self.dispatcher.toc(access, &pass))
    }

    async fn check_strict_mode_batch<'b, I, R>(
        &'b self,
        requests: &[I],
        conv: impl Fn(&I) -> &R,
        collection_name: &str,
        timeout: Option<usize>,
        access: &Access,
    ) -> Result<&'b Arc<TableOfContent>, Status>
    where
        R: StrictModeVerification,
    {
        let pass = check_strict_mode_batch(
            requests.iter().map(conv),
            timeout,
            collection_name,
            self.dispatcher,
            access,
        )
        .await?;
        Ok(self.dispatcher.toc(access, &pass))
    }
}
