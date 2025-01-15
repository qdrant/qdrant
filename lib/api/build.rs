use std::path::PathBuf;
use std::process::Command;
use std::{env, str};

use common::defaults;
use tonic_build::Builder;

fn main() -> std::io::Result<()> {
    // Ensure Qdrant version is configured correctly
    assert_eq!(
        defaults::QDRANT_VERSION.to_string(),
        env!("CARGO_PKG_VERSION"),
        "crate version does not match with defaults.rs",
    );

    let build_out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Build gRPC bits from proto file
    tonic_build::configure()
        // Because we want to attach all validation rules to the generated gRPC types, we must do
        // so by extending the builder. This is ugly, but better than manually implementing
        // `Validation` for all these types and seems to be the best approach. The line below
        // configures all attributes.
        .configure_validation()
        .file_descriptor_set_path(build_out_dir.join("qdrant_descriptor.bin"))
        .out_dir("src/grpc/") // saves generated structures at this location
        .compile(
            &["src/grpc/proto/qdrant.proto"], // proto entry point
            &["src/grpc/proto"], // specify the root location to search proto dependencies
        )?;

    // Append trait extension imports to generated gRPC output
    append_to_file(
        "src/grpc/qdrant.rs",
        "use super::validate::ValidateExt;\nuse validator::Validate;",
    );

    // Fetch git commit ID and pass it to the compiler
    let git_commit_id =
        option_env!("GIT_COMMIT_ID").map(Into::into).or_else(|| {
            match Command::new("git").args(["rev-parse", "HEAD"]).output() {
                Ok(output) if output.status.success() => {
                    Some(str::from_utf8(&output.stdout).unwrap().trim().to_string())
                }
                _ => {
                    println!("cargo:warning=current git commit hash could not be determined");
                    None
                }
            }
        });

    if let Some(commit_id) = git_commit_id {
        println!("cargo:rustc-env=GIT_COMMIT_ID={commit_id}");
    }

    Ok(())
}

/// Extension to [`Builder`] to configure validation attributes.
trait BuilderExt {
    fn configure_validation(self) -> Self;
    fn validates(self, fields: &[(&str, &str)], extra_derives: &[&str]) -> Self;
    fn derive_validate(self, path: &str) -> Self;
    fn derive_validates(self, paths: &[&str]) -> Self;
    fn field_validate(self, path: &str, constraint: &str) -> Self;
    fn field_validates(self, paths: &[(&str, &str)]) -> Self;
}

impl BuilderExt for Builder {
    fn configure_validation(self) -> Self {
        configure_validation(self)
    }

    fn validates(self, fields: &[(&str, &str)], extra_derives: &[&str]) -> Self {
        // Build list of structs to derive validation on, guess these from list of fields
        let mut derives = fields
            .iter()
            .map(|(field, _)| field.split_once('.').unwrap().0)
            .collect::<Vec<&str>>();
        derives.extend(extra_derives);
        derives.sort_unstable();
        derives.dedup();

        self.derive_validates(&derives).field_validates(fields)
    }

    fn derive_validate(self, path: &str) -> Self {
        self.type_attribute(path, "#[derive(validator::Validate)]")
    }

    fn derive_validates(self, paths: &[&str]) -> Self {
        paths.iter().fold(self, |c, path| c.derive_validate(path))
    }

    fn field_validate(self, path: &str, constraint: &str) -> Self {
        if constraint.is_empty() {
            self.field_attribute(path, "#[validate(nested)]")
        } else {
            self.field_attribute(path, format!("#[validate({constraint})]"))
        }
    }

    fn field_validates(self, fields: &[(&str, &str)]) -> Self {
        fields.iter().fold(self, |c, (path, constraint)| {
            c.field_validate(path, constraint)
        })
    }
}

/// Configure additional attributes required for validation on generated gRPC types.
///
/// These are grouped by service file.
#[rustfmt::skip]
fn configure_validation(builder: Builder) -> Builder {
    builder
        // prost_wkt_types needed for serde support
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        // Service: collections.proto
        .validates(&[
            ("GetCollectionInfoRequest.collection_name", "length(min = 1, max = 255)"),
            ("CollectionExistsRequest.collection_name", "length(min = 1, max = 255)"),
            ("CreateCollection.collection_name", "length(min = 1, max = 255), custom(function = \"common::validation::validate_collection_name\")"),
            ("CreateCollection.hnsw_config", ""),
            ("CreateCollection.wal_config", ""),
            ("CreateCollection.optimizers_config", ""),
            ("CreateCollection.vectors_config", ""),
            ("CreateCollection.quantization_config", ""),
            ("CreateCollection.shard_number", "range(min = 1)"),
            ("CreateCollection.replication_factor", "range(min = 1)"),
            ("CreateCollection.write_consistency_factor", "range(min = 1)"),
            ("CreateCollection.strict_mode_config", ""),
            ("UpdateCollection.collection_name", "length(min = 1, max = 255)"),
            ("UpdateCollection.optimizers_config", ""),
            ("UpdateCollection.params", ""),
            ("UpdateCollection.timeout", "range(min = 1)"),
            ("UpdateCollection.hnsw_config", ""),
            ("UpdateCollection.vectors_config", ""),
            ("UpdateCollection.quantization_config", ""),
            ("UpdateCollection.strict_mode_config", ""),
            ("CollectionParamsDiff.replication_factor", "range(min = 1)"),
            ("CollectionParamsDiff.write_consistency_factor", "range(min = 1)"),
            ("DeleteCollection.collection_name", "length(min = 1, max = 255)"),
            ("DeleteCollection.timeout", "range(min = 1)"),
            ("CollectionConfig.params", ""),
            ("CollectionConfig.hnsw_config", ""),
            ("CollectionConfig.optimizers_config", ""),
            ("CollectionConfig.quantization_config", ""),
            ("CollectionConfig.strict_mode_config", ""),
            ("CollectionParams.vectors_config", ""),
            ("ChangeAliases.timeout", "range(min = 1)"),
            ("ListCollectionAliasesRequest.collection_name", "length(min = 1, max = 255)"),
            ("HnswConfigDiff.ef_construct", "range(min = 4)"),
            ("WalConfigDiff.wal_capacity_mb", "range(min = 1)"),
            ("OptimizersConfigDiff.deleted_threshold", "range(min = 0.0, max = 1.0)"),
            ("OptimizersConfigDiff.vacuum_min_vector_number", "range(min = 100)"),
            ("OptimizersConfigDiff.max_segment_size", "range(min = 1)"),
            ("VectorsConfig.config", ""),
            ("VectorsConfigDiff.config", ""),
            ("VectorParams.size", "range(min = 1, max = 65536)"),
            ("VectorParams.hnsw_config", ""),
            ("VectorParams.quantization_config", ""),
            ("VectorParamsMap.map", ""),
            ("VectorParamsDiff.hnsw_config", ""),
            ("VectorParamsDiff.quantization_config", ""),
            ("VectorParamsDiffMap.map", ""),
            ("QuantizationConfig.quantization", ""),
            ("QuantizationConfigDiff.quantization", ""),
            ("ScalarQuantization.quantile", "range(min = 0.5, max = 1.0)"),
            ("UpdateCollectionClusterSetupRequest.timeout", "range(min = 1)"),
            ("UpdateCollectionClusterSetupRequest.operation", ""),
            ("StrictModeConfig.max_query_limit", "range(min = 1)"),
            ("StrictModeConfig.max_timeout", "range(min = 1)"),
            ("StrictModeConfig.read_rate_limit_per_minute", "range(min = 1)"),
            ("StrictModeConfig.write_rate_limit_per_minute", "range(min = 1)"),
        ], &[
            "ListCollectionsRequest",
            "ListAliasesRequest",
            "CollectionClusterInfoRequest",
            "UpdateCollectionClusterSetupRequest",
            "ProductQuantization",
            "BinaryQuantization",
            "Disabled",
            "QuantizationConfigDiff",
            "quantization_config_diff::Quantization",
            "Replica",
        ])
        // Service: collections_internal.proto
        .validates(&[
            ("GetCollectionInfoRequestInternal.get_collection_info_request", ""),
            ("InitiateShardTransferRequest.collection_name", "length(min = 1, max = 255)"),
            ("WaitForShardStateRequest.collection_name", "length(min = 1, max = 255)"),
            ("WaitForShardStateRequest.timeout", "range(min = 1)"),
            ("GetShardRecoveryPointRequest.collection_name", "length(min = 1, max = 255)"),
            ("UpdateShardCutoffPointRequest.collection_name", "length(min = 1, max = 255)"),
        ], &[])
        // Service: points.proto
        .validates(&[
            ("UpsertPoints.collection_name", "length(min = 1, max = 255)"),
            ("UpsertPoints.points", ""),
            ("DeletePoints.collection_name", "length(min = 1, max = 255)"),
            ("UpdatePointVectors.collection_name", "length(min = 1, max = 255)"),
            ("UpdatePointVectors.points", ""),
            ("DeletePointVectors.collection_name", "length(min = 1, max = 255)"),
            ("DeletePointVectors.vector_names", "length(min = 1, message = \"must specify vector names to delete\")"),
            ("PointVectors.vectors", ""),
            ("GetPoints.collection_name", "length(min = 1, max = 255)"),
            ("SetPayloadPoints.collection_name", "length(min = 1, max = 255)"),
            ("DeletePayloadPoints.collection_name", "length(min = 1, max = 255)"),
            ("ClearPayloadPoints.collection_name", "length(min = 1, max = 255)"),
            ("UpdateBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("UpdateBatchPoints.operations", "length(min = 1)"),
            ("CreateFieldIndexCollection.collection_name", "length(min = 1, max = 255)"),
            ("CreateFieldIndexCollection.field_name", "length(min = 1)"),
            ("DeleteFieldIndexCollection.collection_name", "length(min = 1, max = 255)"),
            ("DeleteFieldIndexCollection.field_name", "length(min = 1)"),
            ("SearchPoints.collection_name", "length(min = 1, max = 255)"),
            ("SearchPoints.filter", ""),
            ("SearchPoints.limit", "range(min = 1)"),
            ("SearchPoints.params", ""),
            ("SearchPoints.timeout", "range(min = 1)"),
            ("SearchBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("SearchBatchPoints.search_points", ""),
            ("SearchBatchPoints.timeout", "range(min = 1)"),
            ("SearchPointGroups.collection_name", "length(min = 1, max = 255)"),
            ("SearchPointGroups.group_by", "length(min = 1)"),
            ("SearchPointGroups.filter", ""),
            ("SearchPointGroups.params", ""),
            ("SearchPointGroups.group_size", "range(min = 1)"),
            ("SearchPointGroups.limit", "range(min = 1)"),
            ("SearchPointGroups.timeout", "range(min = 1)"),
            ("SearchParams.quantization", ""),
            ("QuantizationSearchParams.oversampling", "range(min = 1.0)"),
            ("ScrollPoints.collection_name", "length(min = 1, max = 255)"),
            ("ScrollPoints.filter", ""),
            ("ScrollPoints.limit", "range(min = 1)"),
            ("RecommendPoints.collection_name", "length(min = 1, max = 255)"),
            ("RecommendPoints.filter", ""),
            ("RecommendPoints.params", ""),
            ("RecommendPoints.timeout", "range(min = 1)"),
            ("RecommendPoints.positive_vectors", ""),
            ("RecommendPoints.negative_vectors", ""),
            ("RecommendBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("RecommendBatchPoints.recommend_points", ""),
            ("RecommendBatchPoints.timeout", "range(min = 1)"),
            ("RecommendPointGroups.collection_name", "length(min = 1, max = 255)"),
            ("RecommendPointGroups.filter", ""),
            ("RecommendPointGroups.group_by", "length(min = 1)"),
            ("RecommendPointGroups.group_size", "range(min = 1)"),
            ("RecommendPointGroups.limit", "range(min = 1)"),
            ("RecommendPointGroups.params", ""),
            ("RecommendPointGroups.timeout", "range(min = 1)"),
            ("RecommendPointGroups.positive_vectors", ""),
            ("RecommendPointGroups.negative_vectors", ""),
            ("DiscoverPoints.collection_name", "length(min = 1, max = 255)"),
            ("DiscoverPoints.filter", ""),
            ("DiscoverPoints.params", ""),
            ("DiscoverPoints.limit", "range(min = 1)"),
            ("DiscoverPoints.timeout", "range(min = 1)"),
            ("DiscoverBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("DiscoverBatchPoints.discover_points", ""),
            ("DiscoverBatchPoints.timeout", "range(min = 1)"),
            ("CountPoints.collection_name", "length(min = 1, max = 255)"),
            ("CountPoints.filter", ""),
            ("GeoPolygon.exterior", "custom(function = \"crate::grpc::validate::validate_geo_polygon_exterior\")"),
            ("GeoPolygon.interiors", "custom(function = \"crate::grpc::validate::validate_geo_polygon_interiors\")"),
            ("Filter.should", ""),
            ("Filter.must", ""),
            ("Filter.must_not", ""),
            ("NestedCondition.filter", ""),
            ("Condition.condition_one_of", ""),
            ("PointStruct.vectors", ""),
            ("Vectors.vectors_options", ""),
            ("NamedVectors.vectors", ""),
            ("DatetimeRange.lt", "custom(function = \"crate::grpc::validate::validate_timestamp\")"),
            ("DatetimeRange.gt", "custom(function = \"crate::grpc::validate::validate_timestamp\")"),
            ("DatetimeRange.lte", "custom(function = \"crate::grpc::validate::validate_timestamp\")"),
            ("DatetimeRange.gte", "custom(function = \"crate::grpc::validate::validate_timestamp\")"),
            ("QueryPoints.collection_name", "length(min = 1, max = 255)"),
            ("QueryPoints.limit", "range(min = 1)"),
            ("QueryPoints.filter", ""),
            ("QueryPoints.params", ""),
            ("QueryPoints.timeout", "range(min = 1)"),
            ("QueryBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("QueryBatchPoints.query_points", ""),
            ("QueryBatchPoints.timeout", "range(min = 1)"),
            ("FacetCounts.collection_name", "length(min = 1, max = 255)"),
            ("FacetCounts.key", "length(min = 1)"),
            ("FacetCounts.filter", ""),
            ("FacetCounts.timeout", "range(min = 1)"),
            ("SearchMatrixPoints.collection_name", "length(min = 1, max = 255)"),
            ("SearchMatrixPoints.filter", ""),
            ("SearchMatrixPoints.sample", "range(min = 2)"),
            ("SearchMatrixPoints.limit", "range(min = 1)"),
            ("SearchMatrixPoints.timeout", "range(min = 1)")
        ], &[])
        .type_attribute(".", "#[derive(serde::Serialize)]")
        // Service: points_internal_service.proto
        .validates(&[
            ("UpsertPointsInternal.upsert_points", ""),
            ("DeletePointsInternal.delete_points", ""),
            ("UpdateVectorsInternal.update_vectors", ""),
            ("DeleteVectorsInternal.delete_vectors", ""),
            ("SetPayloadPointsInternal.set_payload_points", ""),
            ("DeletePayloadPointsInternal.delete_payload_points", ""),
            ("ClearPayloadPointsInternal.clear_payload_points", ""),
            ("CreateFieldIndexCollectionInternal.create_field_index_collection", ""),
            ("DeleteFieldIndexCollectionInternal.delete_field_index_collection", ""),
            ("SearchPointsInternal.search_points", ""),
            ("SearchBatchPointsInternal.collection_name", "length(min = 1, max = 255)"),
            ("SearchBatchPointsInternal.search_points", ""),
            ("CoreSearchPoints.collection_name", "length(min = 1, max = 255)"),
            ("CoreSearchPoints.filter", ""),
            ("CoreSearchPoints.limit", "range(min = 1)"),
            ("CoreSearchPoints.params", ""),
            ("CoreSearchBatchPointsInternal.collection_name", "length(min = 1, max = 255)"),
            ("CoreSearchBatchPointsInternal.search_points", ""),
            ("RecoQuery.positives", ""),
            ("RecoQuery.negatives", ""),
            ("ContextPair.positive", ""),
            ("ContextPair.negative", ""),
            ("DiscoveryQuery.target", ""),
            ("DiscoveryQuery.context", ""),
            ("ContextQuery.context", ""),
            ("RecommendPointsInternal.recommend_points", ""),
            ("ScrollPointsInternal.scroll_points", ""),
            ("GetPointsInternal.get_points", ""),
            ("CountPointsInternal.count_points", ""),
            ("SyncPointsInternal.sync_points", ""),
            ("SyncPoints.collection_name", "length(min = 1, max = 255)"),
            ("QueryBatchPointsInternal.collection_name", "length(min = 1, max = 255)"),
            ("QueryBatchPointsInternal.timeout", "range(min = 1)"),
            ("FacetCountsInternal.collection_name", "length(min = 1, max = 255)"),
            ("FacetCountsInternal.timeout", "range(min = 1)"),
        ], &[])
        // Service: raft_service.proto
        .validates(&[
            ("AddPeerToKnownMessage.uri", "custom(function = \"common::validation::validate_not_empty\")"),
            ("AddPeerToKnownMessage.port", "range(min = 1)"),
        ], &[])
        // Service: snapshot_service.proto
        .validates(&[
            ("CreateSnapshotRequest.collection_name", "length(min = 1, max = 255)"),
            ("ListSnapshotsRequest.collection_name", "length(min = 1, max = 255)"),
            ("DeleteSnapshotRequest.collection_name", "length(min = 1, max = 255)"),
            ("DeleteSnapshotRequest.snapshot_name", "length(min = 1)"),
            ("DeleteFullSnapshotRequest.snapshot_name", "length(min = 1)"),
            ("CreateShardSnapshotRequest.collection_name", "length(min = 1, max = 255)"),
            ("ListShardSnapshotsRequest.collection_name", "length(min = 1, max = 255)"),
            ("DeleteShardSnapshotRequest.collection_name", "length(min = 1, max = 255)"),
            ("DeleteShardSnapshotRequest.snapshot_name", "length(min = 1)"),
            ("RecoverShardSnapshotRequest.collection_name", "length(min = 1, max = 255)"),
            ("RecoverShardSnapshotRequest.snapshot_name", "length(min = 1)"),
            ("RecoverShardSnapshotRequest.checksum", "custom(function = \"common::validation::validate_sha256_hash\")"),
            ("SnapshotDescription.creation_time", "custom(function = \"crate::grpc::validate::validate_timestamp\")"),
        ], &[
            "CreateFullSnapshotRequest",
            "ListFullSnapshotsRequest",
        ])
}

fn append_to_file(path: &str, line: &str) {
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    writeln!(
        OpenOptions::new().append(true).open(path).unwrap(),
        "{line}",
    )
    .unwrap()
}
