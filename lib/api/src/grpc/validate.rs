// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::borrow::Cow;
use std::collections::HashMap;

use common::validation::{validate_range_generic, validate_shard_different_peers};
use segment::data_types::index::validate_integer_index_params;
use validator::{Validate, ValidationError, ValidationErrors};

use super::qdrant as grpc;

const TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800; // 0001-01-01T00:00:00Z
const TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799; // 9999-12-31T23:59:59Z

pub trait ValidateExt {
    fn validate(&self) -> Result<(), ValidationErrors>;
}

impl Validate for dyn ValidateExt {
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        ValidateExt::validate(self)
    }
}

impl<V> ValidateExt for ::core::option::Option<V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        (&self).validate()
    }
}

impl<V> ValidateExt for &::core::option::Option<V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        self.as_ref().map(Validate::validate).unwrap_or(Ok(()))
    }
}

impl<K, V> ValidateExt for HashMap<K, V>
where
    V: Validate,
{
    #[inline]
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self.values().find_map(|v| v.validate().err()) {
            Some(err) => ValidationErrors::merge(Err(Default::default()), "[]", Err(err)),
            None => Ok(()),
        }
    }
}

impl Validate for grpc::vectors_config::Config {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::vectors_config::Config;
        match self {
            Config::Params(params) => params.validate(),
            Config::ParamsMap(params_map) => params_map.validate(),
        }
    }
}

impl Validate for grpc::vectors_config_diff::Config {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::vectors_config_diff::Config;
        match self {
            Config::Params(params) => params.validate(),
            Config::ParamsMap(params_map) => params_map.validate(),
        }
    }
}

impl Validate for grpc::create_vector_name_request::VectorConfig {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::create_vector_name_request::VectorConfig;
        match self {
            VectorConfig::DenseConfig(dense) => dense.validate(),
            VectorConfig::SparseConfig(sparse) => sparse.validate(),
        }
    }
}

impl Validate for grpc::quantization_config::Quantization {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::quantization_config::Quantization;
        match self {
            Quantization::Scalar(scalar) => scalar.validate(),
            Quantization::Product(product) => product.validate(),
            Quantization::Binary(binary) => binary.validate(),
            Quantization::Turboquant(turbo) => turbo.validate(),
        }
    }
}

impl Validate for grpc::quantization_config_diff::Quantization {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::quantization_config_diff::Quantization;
        match self {
            Quantization::Scalar(scalar) => scalar.validate(),
            Quantization::Product(product) => product.validate(),
            Quantization::Binary(binary) => binary.validate(),
            Quantization::Turboquant(turbo) => turbo.validate(),
            Quantization::Disabled(_) => Ok(()),
        }
    }
}

impl Validate for grpc::update_collection_cluster_setup_request::Operation {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::update_collection_cluster_setup_request::Operation;
        match self {
            Operation::MoveShard(op) => op.validate(),
            Operation::ReplicateShard(op) => op.validate(),
            Operation::AbortTransfer(op) => op.validate(),
            Operation::DropReplica(op) => op.validate(),
            Operation::CreateShardKey(op) => op.validate(),
            Operation::DeleteShardKey(op) => op.validate(),
            Operation::RestartTransfer(op) => op.validate(),
            Operation::ReplicatePoints(op) => op.validate(),
        }
    }
}

impl Validate for grpc::MoveShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

impl Validate for grpc::ReplicateShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

impl Validate for crate::grpc::qdrant::AbortShardTransfer {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

impl Validate for grpc::CreateShardKey {
    fn validate(&self) -> Result<(), ValidationErrors> {
        if self.replication_factor == Some(0) {
            let mut errors = ValidationErrors::new();
            errors.add(
                "replication_factor",
                ValidationError::new("Replication factor must be greater than 0"),
            );
            return Err(errors);
        }

        if self.shards_number == Some(0) {
            let mut errors = ValidationErrors::new();
            errors.add(
                "shards_number",
                ValidationError::new("Shards number must be greater than 0"),
            );
            return Err(errors);
        }

        Ok(())
    }
}

impl Validate for grpc::DeleteShardKey {
    fn validate(&self) -> Result<(), ValidationErrors> {
        Ok(())
    }
}

impl Validate for grpc::RestartTransfer {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

impl Validate for grpc::ReplicatePoints {
    fn validate(&self) -> Result<(), ValidationErrors> {
        if self.from_shard_key != self.to_shard_key {
            return Ok(());
        }

        let mut errors = ValidationErrors::new();
        errors.add(
            "to_shard_key",
            validator::ValidationError::new("must be different from from_shard_key"),
        );
        Err(errors)
    }
}

impl Validate for grpc::condition::ConditionOneOf {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::condition::ConditionOneOf;
        match self {
            ConditionOneOf::Field(field_condition) => field_condition.validate(),
            ConditionOneOf::Nested(nested) => nested.validate(),
            ConditionOneOf::Filter(filter) => filter.validate(),
            ConditionOneOf::IsEmpty(_) => Ok(()),
            ConditionOneOf::HasId(_) => Ok(()),
            ConditionOneOf::IsNull(_) => Ok(()),
            ConditionOneOf::HasVector(_) => Ok(()),
            ConditionOneOf::Slice(slice_condition) => slice_condition.validate(),
        }
    }
}

impl Validate for grpc::SliceCondition {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let grpc::SliceCondition { total, index } = self;
        let mut errors = ValidationErrors::new();
        if *total == 0 {
            errors.add("total", ValidationError::new("must be greater than 0"));
        } else if index >= total {
            errors.add(
                "index",
                ValidationError::new("must be less than the total number of slices"),
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Validate for grpc::update_operation::Update {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::update_operation::Update;
        match self {
            Update::Sync(op) => op.validate(),
            Update::Upsert(op) => op.validate(),
            Update::Delete(op) => op.validate(),
            Update::UpdateVectors(op) => op.validate(),
            Update::DeleteVectors(op) => op.validate(),
            Update::SetPayload(op) => op.validate(),
            Update::OverwritePayload(op) => op.validate(),
            Update::DeletePayload(op) => op.validate(),
            Update::ClearPayload(op) => op.validate(),
            Update::CreateFieldIndex(op) => op.validate(),
            Update::DeleteFieldIndex(op) => op.validate(),
            Update::CreateVectorName(op) => op.validate(),
            Update::DeleteVectorName(op) => op.validate(),
        }
    }
}

impl Validate for grpc::FieldCondition {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let all_fields_none = matches!(
            self,
            grpc::FieldCondition {
                key: _,
                r#match: None,
                range: None,
                datetime_range: None,
                geo_bounding_box: None,
                geo_radius: None,
                geo_polygon: None,
                values_count: None,
                is_empty: None,
                is_null: None,
            },
        );

        let mut errors = ValidationErrors::new();

        if all_fields_none {
            errors.add(
                "match",
                ValidationError::new("At least one field condition must be specified"),
            );
        }

        let grpc::FieldCondition {
            key: _,
            r#match: _,
            range: _,
            datetime_range: _,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = self;

        errors.merge_self("geo_bounding_box", geo_bounding_box.validate());
        errors.merge_self("geo_radius", geo_radius.validate());
        errors.merge_self("geo_polygon", geo_polygon.validate());

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Validate for grpc::Vector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        #[expect(deprecated)]
        let grpc::Vector {
            data,
            indices,
            vectors_count,
            vector,
        } = self;

        if let Some(vector) = vector {
            vector.validate()?;
        }

        match (indices, vectors_count) {
            (Some(_), Some(_)) => {
                let mut errors = ValidationErrors::new();
                errors.add(
                    "indices",
                    ValidationError::new("`indices` and `vectors_count` cannot be both specified"),
                );
                Err(errors)
            }
            (Some(indices), None) => {
                sparse::common::sparse_vector::validate_sparse_vector_impl(&indices.data, data)
            }
            (None, Some(vectors_count)) => {
                common::validation::validate_multi_vector_len(*vectors_count, data)
            }
            (None, None) => Ok(()),
        }
    }
}

impl Validate for grpc::vector::Vector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::vector::Vector::Dense(_dense) => Ok(()),
            grpc::vector::Vector::Sparse(sparse) => sparse.validate(),
            grpc::vector::Vector::MultiDense(multi) => multi.validate(),
            grpc::vector::Vector::Document(_document) => Ok(()),
            grpc::vector::Vector::Image(_image) => Ok(()),
            grpc::vector::Vector::Object(_obj) => Ok(()),
        }
    }
}

impl Validate for grpc::SparseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let grpc::SparseVector { indices, values } = self;
        sparse::common::sparse_vector::validate_sparse_vector_impl(indices, values)
    }
}

impl Validate for grpc::MultiDenseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let grpc::MultiDenseVector { vectors } = self;
        let multivec_length: Vec<_> = vectors.iter().map(|v| v.data.len()).collect();
        common::validation::validate_multi_vector_by_length(&multivec_length)
    }
}

impl Validate for super::qdrant::vectors::VectorsOptions {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            super::qdrant::vectors::VectorsOptions::Vector(v) => v.validate(),
            super::qdrant::vectors::VectorsOptions::Vectors(v) => v.validate(),
        }
    }
}

impl Validate for super::qdrant::query_enum::Query {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            super::qdrant::query_enum::Query::NearestNeighbors(q) => q.validate(),
            super::qdrant::query_enum::Query::RecommendBestScore(q) => q.validate(),
            super::qdrant::query_enum::Query::RecommendSumScores(q) => q.validate(),
            super::qdrant::query_enum::Query::Discover(q) => q.validate(),
            super::qdrant::query_enum::Query::Context(q) => q.validate(),
        }
    }
}

impl Validate for super::qdrant::query::Variant {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::query::Variant::Nearest(q) => q.validate(),
            grpc::query::Variant::NearestWithMmr(q) => q.validate(),
            grpc::query::Variant::Recommend(q) => q.validate(),
            grpc::query::Variant::Discover(q) => q.validate(),
            grpc::query::Variant::Context(q) => q.validate(),
            grpc::query::Variant::Formula(q) => q.validate(),
            grpc::query::Variant::Rrf(q) => q.validate(),
            grpc::query::Variant::RelevanceFeedback(q) => q.validate(),
            grpc::query::Variant::Sample(_)
            | grpc::query::Variant::Fusion(_)
            | grpc::query::Variant::OrderBy(_) => Ok(()),
        }
    }
}

impl Validate for super::qdrant::vector_input::Variant {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::vector_input::Variant::Id(_)
            | grpc::vector_input::Variant::Dense(_)
            | grpc::vector_input::Variant::Document(_)
            | grpc::vector_input::Variant::Image(_)
            | grpc::vector_input::Variant::Object(_) => Ok(()),
            grpc::vector_input::Variant::Sparse(sparse_vector) => sparse_vector.validate(),
            grpc::vector_input::Variant::MultiDense(multi_dense_vector) => {
                multi_dense_vector.validate()
            }
        }
    }
}

impl Validate for super::qdrant::expression::Variant {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::expression::Variant::Constant(_) => Ok(()),
            grpc::expression::Variant::Variable(_) => Ok(()),
            grpc::expression::Variant::Condition(condition) => condition.validate(),
            grpc::expression::Variant::GeoDistance(_) => Ok(()),
            grpc::expression::Variant::Datetime(_) => Ok(()),
            grpc::expression::Variant::DatetimeKey(_) => Ok(()),
            grpc::expression::Variant::Mult(mult_expression) => mult_expression.validate(),
            grpc::expression::Variant::Sum(sum_expression) => sum_expression.validate(),
            grpc::expression::Variant::Div(div_expression) => div_expression.validate(),
            grpc::expression::Variant::Neg(expression) => expression.validate(),
            grpc::expression::Variant::Abs(expression) => expression.validate(),
            grpc::expression::Variant::Sqrt(expression) => expression.validate(),
            grpc::expression::Variant::Pow(pow_expression) => pow_expression.validate(),
            grpc::expression::Variant::Exp(expression) => expression.validate(),
            grpc::expression::Variant::Log10(expression) => expression.validate(),
            grpc::expression::Variant::Ln(expression) => expression.validate(),
            grpc::expression::Variant::ExpDecay(decay_params_expression) => {
                decay_params_expression.validate()
            }
            grpc::expression::Variant::GaussDecay(decay_params_expression) => {
                decay_params_expression.validate()
            }
            grpc::expression::Variant::LinDecay(decay_params_expression) => {
                decay_params_expression.validate()
            }
            grpc::expression::Variant::StrDist(str_dist_params_expression) => {
                str_dist_params_expression.validate()
            }
        }
    }
}

impl Validate for grpc::feedback_strategy::Variant {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::feedback_strategy::Variant::Naive(naive_feedback_strategy) => {
                naive_feedback_strategy.validate()
            }
        }
    }
}

/// Validate that GeoLineString has at least 4 points and is closed.
pub fn validate_geo_polygon_line_helper(line: &grpc::GeoLineString) -> Result<(), ValidationError> {
    let points = &line.points;
    let min_length = 4;
    if points.len() < min_length {
        let mut err: ValidationError = ValidationError::new("min_line_length");
        err.add_param(Cow::from("length"), &points.len());
        err.add_param(Cow::from("min_length"), &min_length);
        return Err(err);
    }

    let first_point = &points[0];
    let last_point = &points[points.len() - 1];
    if first_point != last_point {
        return Err(ValidationError::new("closed_line"));
    }

    Ok(())
}

pub fn validate_geo_polygon_exterior(line: &grpc::GeoLineString) -> Result<(), ValidationError> {
    if line.points.is_empty() {
        return Err(ValidationError::new("not_empty"));
    }
    validate_geo_polygon_line_helper(line)?;
    Ok(())
}

pub fn validate_geo_polygon_interiors(
    lines: &Vec<grpc::GeoLineString>,
) -> Result<(), ValidationError> {
    for line in lines {
        validate_geo_polygon_line_helper(line)?;
    }
    Ok(())
}

/// Reject the `Turbo4` datatype on sparse vector configs.
/// `validator` unwraps `Option<i32>` before calling, so we receive `&i32`.
pub fn validate_sparse_datatype(datatype: &i32) -> Result<(), ValidationError> {
    if *datatype == grpc::Datatype::Turbo4 as i32 {
        return Err(common::validation::sparse_turbo4_unsupported_error());
    }
    Ok(())
}

/// Validate that the timestamp is within the range specified in the protobuf docs.
/// <https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp>
pub fn validate_timestamp(ts: &prost_wkt_types::Timestamp) -> Result<(), ValidationError> {
    validate_range_generic(
        ts.seconds,
        Some(TIMESTAMP_MIN_SECONDS),
        Some(TIMESTAMP_MAX_SECONDS),
    )?;
    validate_range_generic(ts.nanos, Some(0), Some(999_999_999))?;
    Ok(())
}

impl Validate for super::qdrant::payload_index_params::IndexParams {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::payload_index_params::IndexParams::KeywordIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::IntegerIndexParams(integer_index_params) => {
                integer_index_params.validate()
            }
            grpc::payload_index_params::IndexParams::FloatIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::GeoIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::TextIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::BoolIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::DatetimeIndexParams(_) => Ok(()),
            grpc::payload_index_params::IndexParams::UuidIndexParams(_) => Ok(()),
        }
    }
}

impl Validate for super::qdrant::IntegerIndexParams {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let super::qdrant::IntegerIndexParams {
            lookup,
            range,
            is_principal: _,
            on_disk: _,
            enable_hnsw: _,
            memory: _,
        } = &self;
        validate_integer_index_params(lookup, range)
    }
}

impl Validate for super::qdrant::points_selector::PointsSelectorOneOf {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            grpc::points_selector::PointsSelectorOneOf::Points(_) => Ok(()),
            grpc::points_selector::PointsSelectorOneOf::Filter(filter) => filter.validate(),
        }
    }
}

impl Validate for grpc::GeoPoint {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let grpc::GeoPoint { lon, lat } = self;
        segment::types::GeoPoint::validate(*lon, *lat).map_err(|err| {
            let error = ValidationError::new("geo_point").with_message(Cow::Owned(err.to_string()));
            let mut errors = ValidationErrors::new();
            errors.add("geo", error);
            errors
        })
    }
}

#[cfg(test)]
mod tests {
    use validator::Validate;

    use crate::grpc::qdrant::{
        CreateCollection, CreateFieldIndexCollection, CreateVectorNameRequest,
        DenseVectorCreationConfig, FieldCondition, GeoBoundingBox, GeoLineString, GeoPoint,
        GeoPolygon, GeoRadius, SearchPoints, UpdateCollection, create_vector_name_request,
    };
    use crate::grpc::{StrDistFunc, StrDistParamsExpression};

    #[test]
    fn test_geo_field_condition_rejects_out_of_range_coordinates() {
        // Valid coordinates pass.
        let valid = FieldCondition {
            geo_radius: Some(GeoRadius {
                center: Some(GeoPoint {
                    lat: 52.5,
                    lon: 13.4,
                }),
                radius: 1000.0,
            }),
            ..Default::default()
        };
        assert!(valid.validate().is_ok());

        // Latitude out of range (> 90) is rejected instead of panicking in the
        // geo index during geohash encoding.
        let bad_radius = FieldCondition {
            geo_radius: Some(GeoRadius {
                center: Some(GeoPoint {
                    lat: 200.0,
                    lon: 13.4,
                }),
                radius: 1000.0,
            }),
            ..Default::default()
        };
        assert!(bad_radius.validate().is_err());

        // Longitude out of range (< -180) in a bounding-box corner is rejected.
        let bad_box = FieldCondition {
            geo_bounding_box: Some(GeoBoundingBox {
                top_left: Some(GeoPoint {
                    lat: 50.0,
                    lon: -190.0,
                }),
                bottom_right: Some(GeoPoint {
                    lat: 40.0,
                    lon: 13.4,
                }),
            }),
            ..Default::default()
        };
        assert!(bad_box.validate().is_err());

        // A well-formed polygon (>= 4 points, closed ring) whose coordinates are
        // out of range is rejected; shape validation alone would accept it.
        let bad_polygon = FieldCondition {
            geo_polygon: Some(GeoPolygon {
                exterior: Some(GeoLineString {
                    points: vec![
                        GeoPoint { lat: 0.0, lon: 0.0 },
                        GeoPoint {
                            lat: 100.0,
                            lon: 0.0,
                        },
                        GeoPoint { lat: 1.0, lon: 1.0 },
                        GeoPoint { lat: 0.0, lon: 0.0 },
                    ],
                }),
                interiors: vec![],
            }),
            ..Default::default()
        };
        assert!(bad_polygon.validate().is_err());
    }

    #[test]
    fn test_good_request() {
        let bad_request = CreateCollection {
            collection_name: "test_collection".into(),
            timeout: Some(10),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );

        // Collection name validation must not be strict on non-creation
        let bad_request = UpdateCollection {
            collection_name: "no\\path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );

        // Collection name validation must not be strict on non-creation
        let bad_request = UpdateCollection {
            collection_name: "no*path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_ok(),
            "good collection request should not error on validation"
        );
    }

    #[test]
    fn test_bad_collection_request() {
        let bad_request = CreateCollection {
            collection_name: "".into(),
            timeout: Some(0),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );

        // Collection name validation must be strict on creation
        let bad_request = CreateCollection {
            collection_name: "no/path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );

        // Collection name validation must be strict on creation
        let bad_request = CreateCollection {
            collection_name: "no*path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );

        // Collection name validation must still disallow some characters on update
        let bad_request = UpdateCollection {
            collection_name: "no/path".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad collection request should error on validation"
        );
    }

    #[test]
    fn test_bad_index_request() {
        let bad_request = CreateFieldIndexCollection {
            collection_name: "".into(),
            field_name: "".into(),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad index request should error on validation"
        );
    }

    #[test]
    fn test_bad_search_request() {
        let bad_request = SearchPoints {
            collection_name: "".into(),
            limit: 0,
            vector_name: Some("".into()),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );

        let bad_request = SearchPoints {
            limit: 0,
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );

        let bad_request = SearchPoints {
            vector_name: Some("".into()),
            ..Default::default()
        };
        assert!(
            bad_request.validate().is_err(),
            "bad search request should error on validation"
        );
    }

    #[test]
    fn test_geo_polygon() {
        let bad_polygon = GeoPolygon {
            exterior: Some(GeoLineString { points: vec![] }),
            interiors: vec![],
        };
        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let bad_polygon = GeoPolygon {
            exterior: Some(GeoLineString {
                points: vec![
                    GeoPoint { lat: 1., lon: 1. },
                    GeoPoint { lat: 2., lon: 2. },
                    GeoPoint { lat: 3., lon: 3. },
                ],
            }),
            interiors: vec![],
        };
        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let bad_polygon = GeoPolygon {
            exterior: Some(GeoLineString {
                points: vec![
                    GeoPoint { lat: 1., lon: 1. },
                    GeoPoint { lat: 2., lon: 2. },
                    GeoPoint { lat: 3., lon: 3. },
                    GeoPoint { lat: 4., lon: 4. },
                ],
            }),
            interiors: vec![],
        };

        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let bad_polygon = GeoPolygon {
            exterior: Some(GeoLineString {
                points: vec![
                    GeoPoint { lat: 1., lon: 1. },
                    GeoPoint { lat: 2., lon: 2. },
                    GeoPoint { lat: 3., lon: 3. },
                    GeoPoint { lat: 1., lon: 1. },
                ],
            }),
            interiors: vec![GeoLineString {
                points: vec![
                    GeoPoint { lat: 1., lon: 1. },
                    GeoPoint { lat: 2., lon: 2. },
                    GeoPoint { lat: 3., lon: 3. },
                    GeoPoint { lat: 2., lon: 2. },
                ],
            }],
        };

        assert!(
            bad_polygon.validate().is_err(),
            "bad polygon should error on validation"
        );

        let good_polygon = GeoPolygon {
            exterior: Some(GeoLineString {
                points: vec![
                    GeoPoint { lat: 1., lon: 1. },
                    GeoPoint { lat: 2., lon: 2. },
                    GeoPoint { lat: 3., lon: 3. },
                    GeoPoint { lat: 1., lon: 1. },
                ],
            }),
            interiors: vec![],
        };
        assert!(
            good_polygon.validate().is_ok(),
            "good polygon should not error on validation"
        );
    }

    #[test]
    fn test_str_dist_query_validation() {
        let bad_request = StrDistParamsExpression {
            field: String::from("title"),
            query: String::from(""),
            func: StrDistFunc::Levenshtein as i32,
        };
        assert!(
            bad_request.validate().is_err(),
            "empty str_dist query should error on validation"
        );

        let good_request = StrDistParamsExpression {
            field: String::from("title"),
            query: String::from("hello"),
            func: StrDistFunc::JaroWinkler as i32,
        };
        assert!(
            good_request.validate().is_ok(),
            "non-empty str_dist query should not error on validation"
        );
    }

    #[test]
    fn test_field_condition_validates_geo_polygon() {
        use crate::grpc::qdrant::FieldCondition;

        // FieldCondition::validate only checks that at least one field is set; it
        // must also reject a malformed geo polygon, otherwise the invalid shape
        // reaches the geo index and panics. An empty exterior is invalid.
        let bad = FieldCondition {
            key: "location".into(),
            geo_polygon: Some(GeoPolygon {
                exterior: Some(GeoLineString { points: vec![] }),
                interiors: vec![],
            }),
            ..Default::default()
        };
        assert!(
            bad.validate().is_err(),
            "field condition with an empty polygon exterior should error"
        );

        // A well-formed polygon still passes.
        let good = FieldCondition {
            key: "location".into(),
            geo_polygon: Some(GeoPolygon {
                exterior: Some(GeoLineString {
                    points: vec![
                        GeoPoint { lat: 1., lon: 1. },
                        GeoPoint { lat: 2., lon: 2. },
                        GeoPoint { lat: 3., lon: 3. },
                        GeoPoint { lat: 1., lon: 1. },
                    ],
                }),
                interiors: vec![],
            }),
            ..Default::default()
        };
        assert!(good.validate().is_ok());
    }

    #[test]
    fn test_create_vector_name_request() {
        let request_with_zero_size = CreateVectorNameRequest {
            collection_name: "test".into(),
            vector_name: "vec".into(),
            vector_config: Some(create_vector_name_request::VectorConfig::DenseConfig(
                DenseVectorCreationConfig {
                    size: 0,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(request_with_zero_size.validate().is_err());

        let request_with_oversize = CreateVectorNameRequest {
            collection_name: "test".into(),
            vector_name: "vec".into(),
            vector_config: Some(create_vector_name_request::VectorConfig::DenseConfig(
                DenseVectorCreationConfig {
                    size: 65537,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(request_with_oversize.validate().is_err());

        let good_request = CreateVectorNameRequest {
            collection_name: "test".into(),
            vector_name: "vec".into(),
            vector_config: Some(create_vector_name_request::VectorConfig::DenseConfig(
                DenseVectorCreationConfig {
                    size: 768,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(good_request.validate().is_ok());
    }
}
