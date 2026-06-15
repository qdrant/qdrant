use std::path::{Path, PathBuf};

use crate::types::VectorName;

pub const PAYLOAD_INDEX_PATH: &str = "payload_index";
pub const VECTOR_STORAGE_PATH: &str = "vector_storage";
pub const VECTOR_INDEX_PATH: &str = "vector_index";

pub fn get_vector_name_with_prefix(prefix: &str, vector_name: &VectorName) -> String {
    if !vector_name.is_empty() {
        format!("{prefix}-{vector_name}")
    } else {
        prefix.to_owned()
    }
}

pub fn get_vector_storage_path(segment_path: &Path, vector_name: &VectorName) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(
        VECTOR_STORAGE_PATH,
        vector_name,
    ))
}

pub fn get_vector_index_path(segment_path: &Path, vector_name: &VectorName) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(VECTOR_INDEX_PATH, vector_name))
}

pub(crate) fn get_payload_index_path(segment_path: &Path) -> PathBuf {
    segment_path.join(PAYLOAD_INDEX_PATH)
}
