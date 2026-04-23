use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use collection::operations::verification::new_unchecked_verification_pass;
use common::universal_io::UniversalRead;
use storage::content_manager::toc::COLLECTIONS_DIR;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tonic::Status;

use crate::tonic::api::storage_read_api::StorageReadService;

impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageReadService<S> {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher,
            _marker: PhantomData,
        }
    }

    /// Verify at least read access to the collection and resolve a potential
    /// alias to the real collection name.
    ///
    /// Returns the absolute path to the collection directory.
    pub async fn check_and_resolve_collection(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        method: &str,
    ) -> Result<PathBuf, Status> {
        let pass = auth
            .check_collection_access(collection_name, AccessRequirements::new(), method)
            .map_err(Status::from)?;

        let verification_pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(auth, &verification_pass);
        let resolved_name = match toc.get_collection(&pass).await {
            Ok(collection) => collection.name().to_string(),
            Err(_) => collection_name.to_string(),
        };

        Ok(toc
            .storage_path()
            .join(COLLECTIONS_DIR)
            .join(&resolved_name))
    }
}
