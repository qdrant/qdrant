use std::borrow::Cow;
use std::collections::HashMap;

use common::validation::{validate_range_generic, validate_shard_different_peers};
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

impl Validate for grpc::quantization_config::Quantization {
    fn validate(&self) -> Result<(), ValidationErrors> {
        use grpc::quantization_config::Quantization;
        match self {
            Quantization::Scalar(scalar) => scalar.validate(),
            Quantization::Product(product) => product.validate(),
            Quantization::Binary(binary) => binary.validate(),
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
        Ok(())
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
        }
    }
}

impl Validate for grpc::FieldCondition {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let grpc::FieldCondition {
            key: _,
            r#match,
            range,
            datetime_range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count,
            is_empty,
            is_null,
        } = self;

        let all_fields_none = r#match.is_none()
            && range.is_none()
            && datetime_range.is_none()
            && geo_bounding_box.is_none()
            && geo_radius.is_none()
            && geo_polygon.is_none()
            && values_count.is_none()
            && is_empty.is_none()
            && is_null.is_none();

        if all_fields_none {
            let mut errors = ValidationErrors::new();
            errors.add(
                "match",
                ValidationError::new("At least one field condition must be specified"),
            );
            Err(errors)
        } else {
            Ok(())
        }
    }
}

impl Validate for grpc::Vector {
    fn validate(&self) -> Result<(), ValidationErrors> {
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

#[cfg(test)]
mod tests {
    use validator::Validate;

    use crate::grpc::qdrant::{
        CreateCollection, CreateFieldIndexCollection, GeoLineString, GeoPoint, GeoPolygon,
        SearchPoints, UpdateCollection,
    };

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
            collection_name: "no/path".into(),
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
}
