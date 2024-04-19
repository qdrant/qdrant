use std::path::PathBuf;
use std::process::Command;
use std::{env, str};

use common::defaults;
use tonic_build::Builder;

const GRPC_OUTPUT_FILE: &str = "src/grpc/qdrant.rs";

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
        // Similar thing but for `derive_build` attributes. This is needed by the new rust-client
        // to allow a builder pattern style API.
        .configure_derive_builder()
        .file_descriptor_set_path(build_out_dir.join("qdrant_descriptor.bin"))
        .out_dir("src/grpc/") // saves generated structures at this location
        .compile(
            &["src/grpc/proto/qdrant.proto"], // proto entry point
            &["src/grpc/proto"], // specify the root location to search proto dependencies
        )?;

    // Append trait extension imports to generated gRPC output
    append_to_file(GRPC_OUTPUT_FILE, "use super::validate::ValidateExt;");

    // Append macro definition and calls required for builder implementations.
    append_file_to_file(GRPC_OUTPUT_FILE, "./grpc_macros.rs");
    add_builder_macro_impls(GRPC_OUTPUT_FILE, builder_derive_options());

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

/// Derive options for structs. (Path, build attributes, 'from' macro generation enabled)
type BuildDeriveOptions = (&'static str, &'static str, bool);

/// Extension to [`Builder`] to configure validation attributes.
trait BuilderExt {
    fn configure_validation(self) -> Self;
    fn validates(self, fields: &[(&str, &str)], extra_derives: &[&str]) -> Self;
    fn derive_validate(self, path: &str) -> Self;
    fn derive_validates(self, paths: &[&str]) -> Self;
    fn field_validate(self, path: &str, constraint: &str) -> Self;
    fn field_validates(self, paths: &[(&str, &str)]) -> Self;

    fn configure_derive_builder(self) -> Self;
    fn derive_builders(self, paths: &[(&str, &str)], derive_options: &[BuildDeriveOptions])
        -> Self;
    fn derive_builder(self, path: &str, derive_options: Option<&str>) -> Self;
    fn field_build_attributes(self, paths: &[(&str, &str)]) -> Self;
}

impl BuilderExt for Builder {
    fn configure_validation(self) -> Self {
        configure_validation(self)
    }

    fn validates(self, fields: &[(&str, &str)], extra_derives: &[&str]) -> Self {
        // Build list of structs to derive validation on, guess these from list of fields
        let derives = unique_structs_from_paths(fields.iter().map(|i| i.0), extra_derives);
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
            self.field_attribute(path, "#[validate]")
        } else {
            self.field_attribute(path, format!("#[validate({constraint})]"))
        }
    }

    fn field_validates(self, fields: &[(&str, &str)]) -> Self {
        fields.iter().fold(self, |c, (path, constraint)| {
            c.field_validate(path, constraint)
        })
    }

    fn configure_derive_builder(self) -> Self {
        configure_builder(self)
    }

    fn derive_builders(
        self,
        paths: &[(&str, &str)],
        derive_options: &[BuildDeriveOptions],
    ) -> Self {
        let structs = unique_structs_from_paths(paths.iter().map(|i| i.0), &[]);

        let derives = structs.into_iter().fold(self, |c, path| {
            let derive_options = derive_options.iter().find(|i| i.0 == path).map(|i| i.1);
            c.derive_builder(path, derive_options)
        });

        derives.field_build_attributes(paths)
    }

    fn derive_builder(self, path: &str, derive_options: Option<&str>) -> Self {
        let builder = self.type_attribute(path, "#[derive(derive_builder::Builder)]");

        if let Some(derive_options) = derive_options {
            builder.type_attribute(path, format!("#[builder({derive_options})]"))
        } else {
            builder
        }
    }

    fn field_build_attributes(self, paths: &[(&str, &str)]) -> Self {
        paths.iter().fold(self, |c, (path, attribute)| {
            c.field_attribute(path, format!("#[builder({attribute})]"))
        })
    }
}

fn configure_builder(builder: Builder) -> Builder {
    const DEFAULT_OPTION: &str = "default, setter(strip_option)";
    const DEFAULT_OPTION_INTO: &str = "default, setter(into, strip_option)";
    const DEFAULT: &str = "default";

    builder.derive_builders(
        &[
            // VectorParams
            ("VectorParams.size", DEFAULT),
            ("VectorParams.distance", DEFAULT),
            ("VectorParams.hnsw_config", DEFAULT_OPTION_INTO),
            ("VectorParams.quantization_config", DEFAULT_OPTION_INTO),
            ("VectorParams.on_disk", DEFAULT),
            ("VectorParams.datatype", DEFAULT),
            // HnswConfigDiff
            ("HnswConfigDiff.m", DEFAULT_OPTION),
            ("HnswConfigDiff.ef_construct", DEFAULT_OPTION),
            ("HnswConfigDiff.full_scan_threshold", DEFAULT_OPTION),
            ("HnswConfigDiff.max_indexing_threads", DEFAULT_OPTION),
            ("HnswConfigDiff.on_disk", DEFAULT_OPTION),
            ("HnswConfigDiff.payload_m", DEFAULT_OPTION),
        ],
        builder_derive_options(),
    )
}

/// Builder configurations for grpc structs.
fn builder_derive_options() -> &'static [BuildDeriveOptions] {
    const DEFAULT_BUILDER_DERIVE_OPTIONS: &str =
        "build_fn(private, error = \"std::convert::Infallible\", name = \"build_inner\")";

    &[
        ("VectorParams", DEFAULT_BUILDER_DERIVE_OPTIONS, true),
        ("HnswConfigDiff", DEFAULT_BUILDER_DERIVE_OPTIONS, true),
    ]
}

/// Generates all necessary macro calls for builders who should have them.
fn add_builder_macro_impls(file: &str, derive_options: &[BuildDeriveOptions]) {
    let to_append = derive_options
        .iter()
        .filter_map(|i| i.2.then_some(i.0))
        .map(|i| format!("builder_type_conversions!({i}, {i}Builder);\n"))
        .fold(String::new(), |mut s, line| {
            s.push_str(&line);
            s
        });
    append_to_file(file, &to_append);
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
            ("CreateCollection.collection_name", "length(min = 1, max = 255), custom = \"common::validation::validate_collection_name\""),
            ("CreateCollection.hnsw_config", ""),
            ("CreateCollection.wal_config", ""),
            ("CreateCollection.optimizers_config", ""),
            ("CreateCollection.vectors_config", ""),
            ("CreateCollection.quantization_config", ""),
            ("UpdateCollection.collection_name", "length(min = 1, max = 255)"),
            ("UpdateCollection.optimizers_config", ""),
            ("UpdateCollection.params", ""),
            ("UpdateCollection.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("UpdateCollection.hnsw_config", ""),
            ("UpdateCollection.vectors_config", ""),
            ("UpdateCollection.quantization_config", ""),
            ("DeleteCollection.collection_name", "length(min = 1, max = 255)"),
            ("DeleteCollection.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("CollectionConfig.params", ""),
            ("CollectionConfig.hnsw_config", ""),
            ("CollectionConfig.optimizers_config", ""),
            ("CollectionConfig.quantization_config", ""),
            ("CollectionParams.vectors_config", ""),
            ("ChangeAliases.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("ListCollectionAliasesRequest.collection_name", "length(min = 1, max = 255)"),
            ("HnswConfigDiff.ef_construct", "custom = \"crate::grpc::validate::validate_u64_range_min_4\""),
            ("WalConfigDiff.wal_capacity_mb", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("OptimizersConfigDiff.deleted_threshold", "custom = \"crate::grpc::validate::validate_f64_range_1\""),
            ("OptimizersConfigDiff.vacuum_min_vector_number", "custom = \"crate::grpc::validate::validate_u64_range_min_100\""),
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
            ("ScalarQuantization.quantile", "custom = \"crate::grpc::validate::validate_f32_range_min_0_5_max_1\""),
            ("UpdateCollectionClusterSetupRequest.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("UpdateCollectionClusterSetupRequest.operation", ""),
        ], &[
            "ListCollectionsRequest",
            "CollectionParamsDiff",
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
            ("DeletePoints.collection_name", "length(min = 1, max = 255)"),
            ("UpdatePointVectors.collection_name", "length(min = 1, max = 255)"),
            ("UpdatePointVectors.vectors", "custom(function = \"crate::grpc::validate::validate_named_vectors_not_empty\", message = \"must specify vectors to update\")"),
            ("DeletePointVectors.collection_name", "length(min = 1, max = 255)"),
            ("DeletePointVectors.vector_names", "length(min = 1, message = \"must specify vector names to delete\")"),
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
            ("SearchPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("SearchBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("SearchBatchPoints.search_points", ""),
            ("SearchBatchPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("SearchPointGroups.collection_name", "length(min = 1, max = 255)"),
            ("SearchPointGroups.group_by", "length(min = 1)"),
            ("SearchPointGroups.filter", ""),
            ("SearchPointGroups.params", ""),
            ("SearchPointGroups.group_size", "range(min = 1)"),
            ("SearchPointGroups.limit", "range(min = 1)"),
            ("SearchPointGroups.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("SearchParams.quantization", ""),
            ("QuantizationSearchParams.oversampling", "custom = \"crate::grpc::validate::validate_f64_range_min_1\""),
            ("ScrollPoints.collection_name", "length(min = 1, max = 255)"),
            ("ScrollPoints.filter", ""),
            ("ScrollPoints.limit", "custom = \"crate::grpc::validate::validate_u32_range_min_1\""),
            ("RecommendPoints.collection_name", "length(min = 1, max = 255)"),
            ("RecommendPoints.filter", ""),
            ("RecommendPoints.params", ""),
            ("RecommendPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("RecommendPoints.positive_vectors", ""),
            ("RecommendPoints.negative_vectors", ""),
            ("RecommendBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("RecommendBatchPoints.recommend_points", ""),
            ("RecommendBatchPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("RecommendPointGroups.collection_name", "length(min = 1, max = 255)"),
            ("RecommendPointGroups.filter", ""),
            ("RecommendPointGroups.group_by", "length(min = 1)"),
            ("RecommendPointGroups.group_size", "range(min = 1)"),
            ("RecommendPointGroups.limit", "range(min = 1)"),
            ("RecommendPointGroups.params", ""),
            ("RecommendPointGroups.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("RecommendPointGroups.positive_vectors", ""),
            ("RecommendPointGroups.negative_vectors", ""),
            ("DiscoverPoints.collection_name", "length(min = 1, max = 255)"),
            ("DiscoverPoints.filter", ""),
            ("DiscoverPoints.params", ""),
            ("DiscoverPoints.limit", "range(min = 1)"),
            ("DiscoverPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("DiscoverBatchPoints.collection_name", "length(min = 1, max = 255)"),
            ("DiscoverBatchPoints.discover_points", ""),
            ("DiscoverBatchPoints.timeout", "custom = \"crate::grpc::validate::validate_u64_range_min_1\""),
            ("CountPoints.collection_name", "length(min = 1, max = 255)"),
            ("CountPoints.filter", ""),
            ("GeoPolygon.exterior", "custom = \"crate::grpc::validate::validate_geo_polygon_exterior\""),
            ("GeoPolygon.interiors", "custom = \"crate::grpc::validate::validate_geo_polygon_interiors\""),
            ("Filter.should", ""),
            ("Filter.must", ""),
            ("Filter.must_not", ""),
            ("NestedCondition.filter", ""),
            ("Condition.condition_one_of", ""),
            ("Vectors.vectors_options", ""),
            ("NamedVectors.vectors", ""),
            ("RecoQuery.positives", ""),
            ("RecoQuery.negatives", ""),
            ("ContextPair.positive", ""),
            ("ContextPair.negative", ""),
            ("DiscoveryQuery.target", ""),
            ("DiscoveryQuery.context", ""),
            ("ContextQuery.context", ""),
            ("DatetimeRange.lt", "custom = \"crate::grpc::validate::validate_timestamp\""),
            ("DatetimeRange.gt", "custom = \"crate::grpc::validate::validate_timestamp\""),
            ("DatetimeRange.lte", "custom = \"crate::grpc::validate::validate_timestamp\""),
            ("DatetimeRange.gte", "custom = \"crate::grpc::validate::validate_timestamp\""),
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
            ("RecommendPointsInternal.recommend_points", ""),
            ("ScrollPointsInternal.scroll_points", ""),
            ("GetPointsInternal.get_points", ""),
            ("CountPointsInternal.count_points", ""),
            ("SyncPointsInternal.sync_points", ""),
            ("SyncPoints.collection_name", "length(min = 1, max = 255)"),
        ], &[])
        // Service: raft_service.proto
        .validates(&[
            ("AddPeerToKnownMessage.uri", "custom = \"common::validation::validate_not_empty\""),
            ("AddPeerToKnownMessage.port", "custom = \"crate::grpc::validate::validate_u32_range_min_1\""),
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
            ("RecoverShardSnapshotRequest.checksum", "custom = \"common::validation::validate_sha256_hash_option\""),
            ("SnapshotDescription.creation_time", "custom = \"crate::grpc::validate::validate_timestamp\""),
        ], &[
            "CreateFullSnapshotRequest",
            "ListFullSnapshotsRequest",
        ])
}

/// Returns a list of all unique structs that appear in a list of paths.
fn unique_structs_from_paths<'a, I>(paths: I, extra: &[&'a str]) -> Vec<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut derives = paths
        .into_iter()
        .map(|field| field.split_once('.').unwrap().0)
        .collect::<Vec<&str>>();
    derives.extend(extra);
    derives.sort_unstable();
    derives.dedup();
    derives
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

fn append_file_to_file(output: &str, input: &str) {
    let src = std::fs::read_to_string(input).unwrap();
    append_to_file(output, &src);
}
