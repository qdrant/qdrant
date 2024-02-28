use std::num::NonZeroUsize;
use std::time::Duration;

use crate::common::snapshots_manager::S3Config;
use crate::operations::types::NodeType;
use crate::shards::transfer::ShardTransferMethod;

/// Default timeout for search requests.
/// In cluster mode, this should be aligned with collection timeout.
const DEFAULT_SEARCH_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_UPDATE_QUEUE_SIZE: usize = 100;
const DEFAULT_UPDATE_QUEUE_SIZE_LISTENER: usize = 10_000;
pub const DEFAULT_IO_SHARD_TRANSFER_LIMIT: Option<usize> = Some(1);
pub const DEFAULT_SNAPSHOTS_PATH: &str = "./snapshots";

/// Storage configuration shared between all collections.
/// Represents a per-node configuration, which might be changes with restart.
/// Vales of this struct are not persisted.
#[derive(Clone, Debug)]
pub struct SharedStorageConfig {
    pub update_queue_size: usize,
    pub node_type: NodeType,
    pub handle_collection_load_errors: bool,
    pub recovery_mode: Option<String>,
    pub search_timeout: Duration,
    pub update_concurrency: Option<NonZeroUsize>,
    pub is_distributed: bool,
    pub incoming_shard_transfers_limit: Option<usize>,
    pub outgoing_shard_transfers_limit: Option<usize>,
    pub default_shard_transfer_method: Option<ShardTransferMethod>,
    pub snapshots_path: String,
    pub s3_config: Option<S3Config>,
}

impl Default for SharedStorageConfig {
    fn default() -> Self {
        Self {
            update_queue_size: DEFAULT_UPDATE_QUEUE_SIZE,
            node_type: Default::default(),
            handle_collection_load_errors: false,
            recovery_mode: None,
            search_timeout: DEFAULT_SEARCH_TIMEOUT,
            update_concurrency: None,
            is_distributed: false,
            incoming_shard_transfers_limit: DEFAULT_IO_SHARD_TRANSFER_LIMIT,
            outgoing_shard_transfers_limit: DEFAULT_IO_SHARD_TRANSFER_LIMIT,
            default_shard_transfer_method: None,
            snapshots_path: DEFAULT_SNAPSHOTS_PATH.to_string(),
            s3_config: None,
        }
    }
}

impl SharedStorageConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        update_queue_size: Option<usize>,
        node_type: NodeType,
        handle_collection_load_errors: bool,
        recovery_mode: Option<String>,
        search_timeout: Option<Duration>,
        update_concurrency: Option<NonZeroUsize>,
        is_distributed: bool,
        incoming_shard_transfers_limit: Option<usize>,
        outgoing_shard_transfers_limit: Option<usize>,
        default_shard_transfer_method: Option<ShardTransferMethod>,
        snapshots_path: String,
        s3_config: Option<S3Config>,
    ) -> Self {
        let update_queue_size = update_queue_size.unwrap_or(match node_type {
            NodeType::Normal => DEFAULT_UPDATE_QUEUE_SIZE,
            NodeType::Listener => DEFAULT_UPDATE_QUEUE_SIZE_LISTENER,
        });
        Self {
            update_queue_size,
            node_type,
            handle_collection_load_errors,
            recovery_mode,
            search_timeout: search_timeout.unwrap_or(DEFAULT_SEARCH_TIMEOUT),
            update_concurrency,
            is_distributed,
            incoming_shard_transfers_limit,
            outgoing_shard_transfers_limit,
            default_shard_transfer_method,
            snapshots_path,
            s3_config,
        }
    }
}
