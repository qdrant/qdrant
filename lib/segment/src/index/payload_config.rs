use std::collections::HashMap;
use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::types::{PayloadFieldSchema, PayloadKeyType};

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "config.json";

/// Keeps information of which field should be index
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct PayloadConfig {
    pub indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchema>,
}

impl PayloadConfig {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_json(path, self)?)
    }
}
