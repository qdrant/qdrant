mod common;
mod api;

use schemars::{schema_for, JsonSchema};
use serde_json;
use std::path::{Path};
use std::fs::File;
use std::io::Write;

use crate::api::models::CollectionsResponse;
use crate::api::retrieve_api::PointRequest;

use collection::operations::types::{CollectionInfo, Record, SearchRequest, UpdateResult};
use storage::content_manager::storage_ops::StorageOps;
use serde::{Deserialize, Serialize};
use segment::types::ScoredPoint;
use collection::operations::CollectionUpdateOperations;

#[derive(Deserialize, Serialize, JsonSchema)]
struct AllDefinitions {
    a1: CollectionsResponse,
    a2: CollectionInfo,
    a3: StorageOps,
    a4: PointRequest,
    a5: Record,
    a6: SearchRequest,
    a7: ScoredPoint,
    a8: UpdateResult,
    a9: CollectionUpdateOperations,
}


fn save_schema<T:JsonSchema>(path: &Path) {
    let schema = schema_for!(T);
    let mut file = File::create(path).unwrap();

    let schema_str = serde_json::to_string_pretty(&schema).unwrap();
    file.write_all(schema_str.as_bytes()).unwrap();
}


fn main() {
    save_schema::<AllDefinitions>("./openapi/schemas/AllDefinitions.json".as_ref());
}