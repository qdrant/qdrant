use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use collection::shards::shard::ShardId;
use common::universal_io::UniversalRead;
use storage::content_manager::toc::{COLLECTIONS_DIR, TableOfContent};
use storage::rbac::AccessRequirements;
use tonic::Status;

use crate::tonic::api::shard_storage_read_api::ShardStorageReadService;

impl<S: UniversalRead<u8> + Send + Sync + 'static> ShardStorageReadService<S> {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self {
            toc,
            _marker: PhantomData,
        }
    }

    /// Verify at least read access to the collection, resolve a potential
    /// alias to the real collection name, and confirm the requested shard
    /// exists on this peer.
    ///
    /// Returns `(shard_dir, collections_root)`. The collections root is used
    /// as the canonicalization anchor when resolving caller-supplied paths.
    pub async fn check_and_resolve_shard(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        shard_id: ShardId,
        method: &str,
    ) -> Result<(PathBuf, PathBuf), Status> {
        let pass = auth
            .check_collection_access(collection_name, AccessRequirements::new(), method)
            .map_err(Status::from)?;

        let collections_root = self.toc.storage_path().join(COLLECTIONS_DIR);

        let resolved_name = match self.toc.get_collection(&pass).await {
            Ok(collection) => {
                if !collection.contains_shard(shard_id).await {
                    return Err(Status::not_found(format!(
                        "Shard {shard_id} not found in collection '{}' on this peer",
                        collection.name(),
                    )));
                }
                collection.name().to_string()
            }
            // Callers that bypass the TOC (e.g. tests) land here; defer
            // missing-shard detection to the on-disk path resolution below.
            Err(_) => collection_name.to_string(),
        };

        let shard_base = collections_root
            .join(&resolved_name)
            .join(shard_id.to_string());

        Ok((shard_base, collections_root))
    }
}
