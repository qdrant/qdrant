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
pub const DEFAULT_SNAPSHOTS_STORAGE: &str = "local";

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
    pub default_shard_transfer_method: Option<ShardTransferMethod>,
    pub incoming_shard_transfers_limit: Option<usize>,
    pub outgoing_shard_transfers_limit: Option<usize>,
    pub snapshots_path: String,
    pub s3_config: Option<S3Config>,
    pub snapshots_storage: String,
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
            default_shard_transfer_method: None,
            incoming_shard_transfers_limit: DEFAULT_IO_SHARD_TRANSFER_LIMIT,
            outgoing_shard_transfers_limit: DEFAULT_IO_SHARD_TRANSFER_LIMIT,
            snapshots_path: DEFAULT_SNAPSHOTS_PATH.to_string(),
            s3_config: None,
            snapshots_storage: DEFAULT_SNAPSHOTS_STORAGE.to_string(),
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
        default_shard_transfer_method: Option<ShardTransferMethod>,
        incoming_shard_transfers_limit: Option<usize>,
        outgoing_shard_transfers_limit: Option<usize>,
        snapshots_path: String,
        s3_config: Option<S3Config>,
        snapshots_storage: String,
    ) -> Self {
        let update_queue_size = update_queue_size.unwrap_or(match node_type {
            NodeType::Normal => DEFAULT_UPDATE_QUEUE_SIZE,
            NodeType::Listener => DEFAULT_UPDATE_QUEUE_SIZE_LISTENER,
        });
        let snapshots_storage = match snapshots_storage.as_str() {
            "s3" => String::from("s3"),
            _ => String::from("local"), // Default to "local" if an invalid option is provided
        };
        Self {
            update_queue_size,
            node_type,
            handle_collection_load_errors,
            recovery_mode,
            search_timeout: search_timeout.unwrap_or(DEFAULT_SEARCH_TIMEOUT),
            update_concurrency,
            is_distributed,
            default_shard_transfer_method,
            incoming_shard_transfers_limit,
            outgoing_shard_transfers_limit,
            snapshots_path,
            s3_config,
            snapshots_storage,
        }
    }
}
