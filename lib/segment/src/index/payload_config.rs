use crate::common::file_operations::{atomic_save_json, read_json};
use crate::entry::entry_point::OperationResult;
use crate::types::PayloadKeyType;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "config.json";

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct PayloadConfig {
    pub indexed_fields: Vec<PayloadKeyType>,
}

impl PayloadConfig {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        read_json(path)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        atomic_save_json(path, self)
    }
}
