use std::collections::BTreeMap;

use api::grpc::models::CollectionsResponse;
use collection::operations::cluster_ops::ClusterOperations;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::payload_ops::{DeletePayload, SetPayload};
use collection::operations::point_ops::{PointInsertOperations, PointsSelector, WriteOrdering};
use collection::operations::snapshot_ops::{SnapshotDescription, SnapshotRecover};
use collection::operations::types::{
    AliasDescription, CollectionClusterInfo, CollectionInfo, CollectionsAliasesResponse,
    CountRequest, CountResult, PointRequest, RecommendRequest, RecommendRequestBatch, Record,
    ScrollRequest, ScrollResult, SearchRequest, SearchRequestBatch, UpdateResult,
};
use schemars::gen::SchemaSettings;
use schemars::JsonSchema;
use segment::types::ScoredPoint;
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CreateCollection, UpdateCollection,
};
use storage::types::ClusterStatus;
use utoipa::openapi::{RefOr, Schema};
use utoipa::OpenApi;

use crate::actix::api_doc::ApiDoc;
use crate::common::helpers::LocksOption;
use crate::common::points::CreateFieldIndex;
use crate::common::telemetry::TelemetryData;

mod actix;
mod common;
mod settings;

#[derive(Deserialize, Serialize, JsonSchema)]
struct AllDefinitions {
    // a1: CollectionsResponse,
    // a2: CollectionInfo,
    // // a3: CollectionMetaOperations,
    // a4: PointRequest,
    // a5: Record,
    // a6: SearchRequest,
    // a7: ScoredPoint,
    // a8: UpdateResult,
    // // a9: CollectionUpdateOperations,
    // aa: RecommendRequest,
    // ab: ScrollRequest,
    // ac: ScrollResult,
    // ad: CreateCollection,
    // ae: UpdateCollection,
    // af: ChangeAliasesOperation,
    // ag: CreateFieldIndex,
    // ah: PointsSelector,
    // ai: PointInsertOperations,
    // aj: SetPayload,
    // ak: DeletePayload,
    al: ClusterStatus,
    // am: SnapshotDescription,
    // an: CountRequest,
    // ao: CountResult,
    // ap: CollectionClusterInfo,
    // aq: TelemetryData,
    // ar: ClusterOperations,
    // at: SearchRequestBatch,
    // au: RecommendRequestBatch,
    // av: LocksOption,
    // aw: SnapshotRecover,
    // ax: CollectionsAliasesResponse,
    // ay: AliasDescription,
    // az: WriteOrdering,
    // b1: ReadConsistency,
    b2: bool,
}

#[derive(Deserialize)]
struct AllDefinitionsSchema {
    definitions: BTreeMap<String, RefOr<Schema>>,
}

fn schema<T: JsonSchema>() -> Result<String, serde_json::Error> {
    let settings = SchemaSettings::openapi3();
    let gen = settings.into_generator();
    let schema = gen.into_root_schema_for::<T>();
    serde_json::to_string_pretty(&schema)
}

fn generate_openapi<T: JsonSchema>() -> Result<String, serde_json::Error> {
    let schema_str = schema::<T>()?;

    let wrapper: AllDefinitionsSchema = serde_json::from_str(&schema_str)?;

    let mut components = utoipa::openapi::schema::Components::default();
    components.schemas = wrapper.definitions;

    let mut schemars_openapi = utoipa::openapi::OpenApi::default();
    schemars_openapi.components = Some(components);

    let mut openapi = ApiDoc::openapi();
    openapi.merge(schemars_openapi);

    openapi.to_pretty_json()
}

fn main() {
    // TODO: Update scripts to make this the only openapi generator
    let openapi_str = generate_openapi::<AllDefinitions>().unwrap();
    println!("{openapi_str}");
}
