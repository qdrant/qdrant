//! Storage - is a crate which contains all service functions, abstracted from the external interface
//!
//! It provides all functions, which could be used from REST (or any other interface), but do not
//! implement any concrete interface.

pub mod content_manager;
pub mod types;

pub mod serialize_peer_addresses {
    use itertools::Itertools;
    use serde::{self, de, Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    use crate::types::PeerAddressById;

    pub fn serialize<S>(addresses: &PeerAddressById, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let addresses: HashMap<u64, String> = addresses
            .clone()
            .into_iter()
            .map(|(id, address)| (id, format!("{address}")))
            .collect();
        addresses.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerAddressById, D::Error>
    where
        D: Deserializer<'de>,
    {
        let addresses: HashMap<u64, String> = HashMap::deserialize(deserializer)?;
        addresses
            .into_iter()
            .map(|(id, address)| address.parse().map(|address| (id, address)))
            .try_collect()
            .map_err(|err| de::Error::custom(format!("Failed to parse uri: {err}")))
    }
}
