use serde::Deserialize;
use validator::Validate;

use crate::types::{QuantizationConfig, StrictModeConfig, VectorsConfigDefaults};

/// Collection default values
#[derive(Debug, Deserialize, Validate, Clone, PartialEq, Eq)]
pub struct CollectionConfigDefaults {
    #[serde(default)]
    pub vectors: Option<VectorsConfigDefaults>,

    #[validate(nested)]
    pub quantization: Option<QuantizationConfig>,

    #[validate(range(min = 1))]
    pub shard_number: Option<u32>,

    #[validate(range(min = 1))]
    pub shard_number_per_node: Option<u32>,

    #[validate(range(min = 1))]
    pub replication_factor: Option<u32>,

    #[validate(range(min = 1))]
    pub write_consistency_factor: Option<u32>,

    #[validate(nested)]
    pub strict_mode: Option<StrictModeConfig>,
}

impl CollectionConfigDefaults {
    pub fn get_shard_number(&self, number_of_peers: u32) -> u32 {
        match (self.shard_number, self.shard_number_per_node) {
            (Some(shard_number), None) => shard_number,
            (None, Some(shard_number_per_node)) => shard_number_per_node * number_of_peers,
            (None, None) => number_of_peers,
            (Some(shard_number), Some(_)) => {
                log::warn!(
                    "Both shard_number and shard_number_per_node are set. Using shard_number: {shard_number}"
                );
                shard_number
            }
        }
    }
}
