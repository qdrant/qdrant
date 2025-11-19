use std::collections::{HashMap, HashSet};
use std::ops;

use ahash::AHashMap;
use itertools::Itertools;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use crate::shards::shard::ShardId;

/// Shard key mapping type
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(from = "SerdeHelper", into = "SerdeHelper")]
pub struct ShardKeyMapping {
    shard_key_to_shard_ids: HashMap<ShardKey, HashSet<ShardId>>,

    /// `true` if the ShardKeyMapping was specified in the old format.
    /// TODO(shardkey): Remove once all keys are migrated.
    #[serde(skip)]
    pub(crate) was_old_format: bool,
}

impl ops::Deref for ShardKeyMapping {
    type Target = HashMap<ShardKey, HashSet<ShardId>>;

    fn deref(&self) -> &Self::Target {
        &self.shard_key_to_shard_ids
    }
}

impl ops::DerefMut for ShardKeyMapping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shard_key_to_shard_ids
    }
}

impl ShardKeyMapping {
    /// Get an inverse mapping, all shard IDs and their key
    pub fn shard_id_to_shard_key(&self) -> AHashMap<ShardId, ShardKey> {
        self.shard_key_to_shard_ids
            .iter()
            .flat_map(|(shard_key, shard_ids)| {
                shard_ids
                    .iter()
                    .map(|shard_id| (*shard_id, shard_key.clone()))
            })
            .collect()
    }

    /// Return all shard IDs from the mappings
    pub fn shard_ids(&self) -> Vec<ShardId> {
        let ids = self.iter_shard_ids().collect::<Vec<_>>();

        debug_assert!(
            ids.iter().all_unique(),
            "shard mapping contains duplicate shard IDs",
        );

        ids
    }

    /// Iterate over all shard IDs from the mappings
    pub fn iter_shard_ids(&self) -> impl Iterator<Item = ShardId> {
        self.shard_key_to_shard_ids
            .values()
            .flat_map(|shard_ids| shard_ids.iter().copied())
    }

    /// Get the shard key for a given shard ID
    ///
    /// `None` is returned if the shard ID has no key, or if the shard ID is unknown
    pub fn shard_key(&self, shard_id: ShardId) -> Option<ShardKey> {
        self.shard_key_to_shard_ids
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&shard_id))
            .map(|(key, _)| key.clone())
    }
}

impl From<SerdeHelper> for ShardKeyMapping {
    fn from(helper: SerdeHelper) -> Self {
        let mut was_old_format = false;

        let shard_key_to_shard_ids = match helper {
            SerdeHelper::New(key_ids_pairs) => key_ids_pairs
                .into_iter()
                .map(KeyIdsPair::into_parts)
                .collect(),

            SerdeHelper::Old(key_ids_map) => {
                was_old_format = true;
                key_ids_map
            }
        };

        Self {
            shard_key_to_shard_ids,
            was_old_format,
        }
    }
}

/// Helper structure for persisting shard key mapping
///
/// The original format of persisting shard key mappings as hash map is broken. It forgets type
/// information for the shard key, which resulted in shard key numbers to be converted into
/// strings.
///
/// Bug: <https://github.com/qdrant/qdrant/pull/5838>
#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum SerdeHelper {
    New(Vec<KeyIdsPair>),
    // TODO(1.15): remove this old format, deployment should exclusively be using new format
    Old(HashMap<ShardKey, HashSet<ShardId>>),
}

impl From<ShardKeyMapping> for SerdeHelper {
    fn from(mapping: ShardKeyMapping) -> Self {
        let key_ids_pairs = mapping
            .shard_key_to_shard_ids
            .into_iter()
            .map(KeyIdsPair::from)
            .collect();
        Self::New(key_ids_pairs)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyIdsPair {
    /// Shard key
    ///
    /// The key is persisted as untagged variant. The JSON value gives us enough information
    /// however to distinguish between a shard key number and string.
    key: ShardKey,
    /// Associalted shard IDs.
    shard_ids: HashSet<ShardId>,
}

impl KeyIdsPair {
    fn into_parts(self) -> (ShardKey, HashSet<ShardId>) {
        let Self { key, shard_ids } = self;
        (key, shard_ids)
    }
}

impl From<(ShardKey, HashSet<ShardId>)> for KeyIdsPair {
    fn from((key, shard_ids): (ShardKey, HashSet<ShardId>)) -> Self {
        Self { key, shard_ids }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use common::budget::ResourceBudget;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use fs_err::File;
    use segment::types::{PayloadFieldSchema, PayloadSchemaType};
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::collection::{Collection, RequestShardTransfer};
    use crate::config::{CollectionConfigInternal, CollectionParams, ShardingMethod, WalConfig};
    use crate::operations::shared_storage_config::SharedStorageConfig;
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::channel_service::ChannelService;
    use crate::shards::collection_shard_distribution::CollectionShardDistribution;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState, ReplicaState};
    use crate::shards::shard_holder::SHARD_KEY_MAPPING_FILE;

    const COLLECTION_TEST_NAME: &str = "shard_key_test";

    async fn make_collection(collection_name: &str, collection_dir: &TempDir) -> Collection {
        let wal_config = WalConfig::default();
        let mut collection_params = CollectionParams::empty();
        collection_params.sharding_method = Some(ShardingMethod::Custom);

        let config = CollectionConfigInternal {
            params: collection_params,
            optimizer_config: OptimizersConfig::fixture(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: Default::default(),
            strict_mode_config: None,
            uuid: None,
            metadata: None,
        };

        let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

        let collection = Collection::new(
            collection_name.to_string(),
            0,
            collection_dir.path(),
            snapshots_path.path(),
            &config,
            Arc::new(SharedStorageConfig::default()),
            CollectionShardDistribution::all_local(None, 0),
            None,
            ChannelService::default(),
            dummy_on_replica_failure(),
            dummy_request_shard_transfer(),
            dummy_abort_shard_transfer(),
            None,
            None,
            ResourceBudget::default(),
            None,
        )
        .await
        .expect("Failed to create new fixture collection");

        collection
            .create_payload_index(
                "field".parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
                HwMeasurementAcc::new(),
            )
            .await
            .expect("failed to create payload index");

        collection
    }

    pub fn dummy_on_replica_failure() -> ChangePeerFromState {
        Arc::new(move |_peer_id, _shard_id, _from_state| {})
    }

    pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
        Arc::new(move |_transfer| {})
    }

    pub fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_transfer, _reason| {})
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_shard_key_migration() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

        {
            let collection = make_collection(COLLECTION_TEST_NAME, &collection_dir).await;
            collection
                .create_shard_key(
                    ShardKey::Keyword("helloworld".into()),
                    vec![vec![]],
                    ReplicaState::Active,
                )
                .await
                .unwrap();
        }

        let shard_mapping_file = collection_dir.path().join(SHARD_KEY_MAPPING_FILE);

        let shard_key_data: SerdeHelper = {
            let file = File::open(&shard_mapping_file).unwrap();
            serde_json::from_reader(file).unwrap()
        };

        let shard_key_data = ShardKeyMapping::from(shard_key_data);

        // Ensure we have at least one shard key.
        assert!(!shard_key_data.is_empty());

        // Convert to old shard key and overwrite file on disk.
        {
            let old_shard_key_data = SerdeHelper::Old(shard_key_data.shard_key_to_shard_ids);
            let mut writer = File::create(&shard_mapping_file).unwrap();
            serde_json::to_writer(&mut writer, &old_shard_key_data).unwrap();
        }

        // Ensure on disk is now the old version.
        {
            let shard_key_data: SerdeHelper =
                serde_json::from_reader(File::open(&shard_mapping_file).unwrap()).unwrap();

            assert!(matches!(shard_key_data, SerdeHelper::Old(..)));
        }

        let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

        // Load collection once to trigger mirgation to the new shard-key format.
        {
            Collection::load(
                COLLECTION_TEST_NAME.to_string(),
                0,
                collection_dir.path(),
                snapshots_path.path(),
                Default::default(),
                ChannelService::default(),
                dummy_on_replica_failure(),
                dummy_request_shard_transfer(),
                dummy_abort_shard_transfer(),
                None,
                None,
                ResourceBudget::default(),
                None,
            )
            .await;
        }

        let shard_key_data: SerdeHelper =
            { serde_json::from_reader(File::open(&shard_mapping_file).unwrap()).unwrap() };

        // Now we have the new key on disk!
        assert!(matches!(shard_key_data, SerdeHelper::New(..)));
    }
}
