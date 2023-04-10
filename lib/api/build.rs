use tonic_build::Builder;

fn main() -> std::io::Result<()> {
    tonic_build::configure()
        .configure_validation()
        .out_dir("src/grpc/") // saves generated structures at this location
        .compile(
            &["src/grpc/proto/qdrant.proto"], // proto entry point
            &["src/grpc/proto"], // specify the root location to search proto dependencies
        )
}

trait BuilderExt {
    fn configure_validation(self) -> Self;
    fn derive_validate(self, path: &str) -> Self;
    fn derive_validates(self, paths: &[&str]) -> Self;
    fn field_validate(self, path: &str, constraint: &str) -> Self;
    fn field_validates(self, paths: &[(&str, &str)]) -> Self;
}

impl BuilderExt for Builder {
    fn configure_validation(self) -> Self {
        configure_validation(self)
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

#[rustfmt::skip]
fn configure_validation(builder: Builder) -> Builder {
    builder
        .derive_validates(&[
            // points_api.rs
            "UpsertPoints",
            "DeletePoints",
            "GetPoints",
            "SetPayloadPoints",
            "DeletePayloadPoints",
            "ClearPayloadPoints",
            "CreateFieldIndexCollection",
            "DeleteFieldIndexCollection",
            "SearchPoints",
            "SearchBatchPoints",
            "ScrollPoints",
            "RecommendPoints",
            "RecommendBatchPoints",
            "CountPoints",
        ])
        .field_validates(&[
            // points_api.rs
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
            // ("SearchPoints.vector_name", "length(min = 1)"),
            ("SearchBatchPoints.collection_name", "length(min = 1)"),
            // ("SearchBatchPoints.search_points", ""),
            ("ScrollPoints.collection_name", "length(min = 1)"),
            // ("ScrollPoints.limit", "range(min = 1)"),
            ("RecommendPoints.collection_name", "length(min = 1)"),
            ("RecommendPoints.limit", "range(min = 1)"),
            ("RecommendBatchPoints.collection_name", "length(min = 1)"),
            // ("RecommendBatchPoints.recommend_points", ""),
            ("CountPoints.collection_name", "length(min = 1)"),
        ])
}
