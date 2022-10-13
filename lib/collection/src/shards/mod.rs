pub mod channel_service;
pub mod collection_shard_distribution;
mod conversions;
pub mod forward_proxy_shard;
pub mod local_shard;
pub mod local_shard_operations;
pub mod proxy_shard;
pub mod remote_shard;
#[allow(dead_code)]
pub mod replica_set;
pub mod shard;
pub mod shard_config;
pub mod shard_holder;
pub mod shard_trait;
pub mod shard_versioning;
pub mod transfer;

use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use shard::{PeerId, ShardId};

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard_versioning::suggest_next_version_path;

pub const HASH_RING_SHARD_SCALE: u32 = 100;

pub type CollectionId = String;

pub type ShardVersion = usize;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, the is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
}

pub async fn create_shard_dir(
    collection_path: &Path,
    shard_id: ShardId,
) -> CollectionResult<PathBuf> {
    loop {
        let shard_path = suggest_next_version_path(collection_path, shard_id).await?;

        match tokio::fs::create_dir(&shard_path).await {
            Ok(_) => return Ok(shard_path),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    continue;
                } else {
                    return Err(CollectionError::from(e));
                }
            }
        }
    }
}
