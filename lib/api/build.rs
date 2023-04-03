use tonic_build::Builder;

fn main() -> std::io::Result<()> {
    tonic_build::configure()
        .configure_validation()
        .out_dir("src/grpc/") // saves generated structures at this location
        .compile(
            &["src/grpc/proto/qdrant.proto"], // proto entry point
            &["src/grpc/proto"], // specify the root location to search proto dependencies
        )?;

    // Append trait imports to generated gRPC output
    // TODO: find a better way to do this?
    append_file("src/grpc/qdrant.rs", "use super::validate::ValidateExt;");

    Ok(())
}

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
}

fn append_file(path: &str, line: &str) {
    use std::fs::OpenOptions;
    use std::io::prelude::*;

    writeln!(
        OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap(),
        "{line}",
    )
    .expect("failed to append to generated file");
}

#[rustfmt::skip]
fn configure_validation(builder: Builder) -> Builder {
    builder
        // API: collections_api.rs
        .validates(&[
            ("GetCollectionInfoRequest.collection_name", "length(min = 1)"),
            ("CreateCollection.hnsw_config", ""),
            // TODO: ("HnswConfigDiff.m", "range(min = 4, max = 10_000)"),
            // TODO: ("HnswConfigDiff.ef_construct", "range(min = 4)"),
            ("UpdateCollection.collection_name", "length(min = 1)"),
            ("UpdateCollection.optimizers_config", ""),
            // TODO: ("OptimizersConfigDiff.deleted_threshold", "range(min = 0.0, max = 1.0)"),
            // TODO: ("OptimizersConfigDiff.vacuum_min_vector_number", "range(min = 100)"),
            // TODO: ("OptimizersConfigDiff.memmap_threshold", "range(min = 1000)"),
            // TODO: ("OptimizersConfigDiff.indexing_threshold", "range(min = 1000)"),
            // TODO: ("UpdateCollection.timeout", "range(min = 1)"),
            ("UpdateCollection.params", ""),
            ("DeleteCollection.collection_name", "length(min = 1)"),
            // TODO: ("DeleteCollection.timeout", "range(min = 1)"),
            // TODO: ("ChangeAliases.timeout", "range(min = 1)"),
            ("ListCollectionAliasesRequest.collection_name", "length(min = 1)"),
        ], &[
            "ListCollectionsRequest",
            "HnswConfigDiff",
            "OptimizersConfigDiff",
            "CollectionParamsDiff",
            "ChangeAliases",
            "ListAliasesRequest",
        ])
        // API: points_api.rs
        .validates(&[
            ("UpsertPoints.collection_name", "length(min = 1)"),
            ("DeletePoints.collection_name", "length(min = 1)"),
            ("GetPoints.collection_name", "length(min = 1)"),
            ("SetPayloadPoints.collection_name", "length(min = 1)"),
            ("DeletePayloadPoints.collection_name", "length(min = 1)"),
            ("ClearPayloadPoints.collection_name", "length(min = 1)"),
            ("CreateFieldIndexCollection.collection_name", "length(min = 1)"),
            ("CreateFieldIndexCollection.field_name", "length(min = 1)"),
            ("DeleteFieldIndexCollection.collection_name", "length(min = 1)"),
            ("DeleteFieldIndexCollection.field_name", "length(min = 1)"),
            ("SearchPoints.collection_name", "length(min = 1)"),
            ("SearchPoints.limit", "range(min = 1)"),
            // TODO: ("SearchPoints.vector_name", "length(min = 1)"),
            ("SearchBatchPoints.collection_name", "length(min = 1)"),
            ("SearchBatchPoints.search_points", ""),
            ("ScrollPoints.collection_name", "length(min = 1)"),
            // TODO: ("ScrollPoints.limit", "range(min = 1)"),
            ("RecommendPoints.collection_name", "length(min = 1)"),
            ("RecommendBatchPoints.collection_name", "length(min = 1)"),
            ("RecommendBatchPoints.recommend_points", ""),
            ("CountPoints.collection_name", "length(min = 1)"),
            ("SyncPoints.collection_name", "length(min = 1)"),
        ], &[])
        // API: raft_api.rs
        .validates(&[
            // TODO: ("AddPeerToKnownMessage.uri", "length(min = 1)"),
            // TODO: ("AddPeerToKnownMessage.port", "range(min = 1)"),
        ], &[
            "AddPeerToKnownMessage",
        ])
        // API: snapshots_api.rs
        .validates(&[
            ("CreateSnapshotRequest.collection_name", "length(min = 1)"),
            ("ListSnapshotsRequest.collection_name", "length(min = 1)"),
            ("DeleteSnapshotRequest.collection_name", "length(min = 1)"),
            ("DeleteSnapshotRequest.snapshot_name", "length(min = 1)"),
            ("DeleteFullSnapshotRequest.snapshot_name", "length(min = 1)"),
        ], &[
            "CreateFullSnapshotRequest",
            "ListFullSnapshotsRequest",
        ])
}
