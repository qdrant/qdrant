use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};

use collection::operations::types::{
    CollectionInfo, RecommendRequest, Record, ScrollRequest, ScrollResult, SearchRequest,
    UpdateResult,
};
use collection::operations::CollectionUpdateOperations;
use segment::types::ScoredPoint;
use storage::content_manager::storage_ops::StorageOperations;

use crate::actix::api::retrieve_api::PointRequest;
use crate::common::models::CollectionsResponse;

mod actix;
mod common;
mod settings;

#[derive(Deserialize, Serialize, JsonSchema)]
struct AllDefinitions {
    a1: CollectionsResponse,
    a2: CollectionInfo,
    a3: StorageOperations,
    a4: PointRequest,
    a5: Record,
    a6: SearchRequest,
    a7: ScoredPoint,
    a8: UpdateResult,
    a9: CollectionUpdateOperations,
    aa: RecommendRequest,
    ab: ScrollRequest,
    ac: ScrollResult,
}

fn save_schema<T: JsonSchema>() {
    let schema = schema_for!(T);
    let schema_str = serde_json::to_string_pretty(&schema).unwrap();
    println!("{}", schema_str)
}

fn main() {
    save_schema::<AllDefinitions>();
}
