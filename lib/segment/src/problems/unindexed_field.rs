use http::{HeaderMap, Method, Uri};
use issues::{Action, CodeType, ImmediateSolution, Issue, Solution};
use itertools::Itertools;

use crate::common::operation_error::OperationError;
use crate::data_types::text_index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::types::{
    AnyVariants, FieldCondition, Match, MatchValue, PayloadFieldSchema, PayloadSchemaParams,
    PayloadSchemaType,
};

pub struct UnindexedField {
    field_name: String,
    field_schemas: Vec<PayloadFieldSchema>,
    collection_name: String,
}
impl UnindexedField {
    pub fn get_code(collection_name: &str, field_name: &str) -> CodeType {
        format!("UNINDEXED_FIELD:{collection_name}:{field_name}")
    }
}
impl Issue for UnindexedField {
    fn code(&self) -> CodeType {
        Self::get_code(&self.collection_name, &self.field_name)
    }

    fn description(&self) -> String {
        format!(
            "Unindexed field '{}' is slowing down queries in collection '{}'",
            self.field_name, self.collection_name
        )
    }

    fn solution(&self) -> Solution {
        let uri = match Uri::builder()
            .path_and_query(format!("/collections/{}/index", self.collection_name).as_str())
            .build()
        {
            Ok(uri) => uri,
            Err(e) => {
                log::warn!("Failed to build uri: {}", e);
                return Solution::None;
            }
        };

        let mut solutions = self.field_schemas.iter().cloned().map(|field_schema| {
            let request_body = serde_json::json!({
                "field_name": self.field_name,
                "field_schema": field_schema,
            });
            ImmediateSolution {
                message: format!(
                    "Create an index on field '{}' of schema '{}' in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
                    self.field_name, serde_json::to_string(&field_schema).unwrap(), self.collection_name
                ),
                action: Action {
                    method: Method::PUT,
                    uri: uri.clone(),
                    headers: HeaderMap::new(),
                    body: Some(request_body),
                },
            }
        }).collect_vec();

        match solutions.len() {
            0 => Solution::None,
            1 => Solution::Immediate(solutions.pop().unwrap()),
            _ => Solution::ImmediateChoice(solutions),
        }
    }
}

fn infer_type_from_match_value(value: &MatchValue) -> PayloadFieldSchema {
    match &value.value {
        crate::types::ValueVariants::Keyword(_string) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)
        }
        crate::types::ValueVariants::Integer(_integer) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)
        }
        crate::types::ValueVariants::Bool(_boolean) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Bool)
        }
    }
}

fn infer_type_from_any_variants(value: &AnyVariants) -> PayloadFieldSchema {
    match value {
        AnyVariants::Keywords(_strings) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)
        }
        AnyVariants::Integers(_integers) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)
        }
    }
}

fn infer_type_from_field_condition(field_condition: &FieldCondition) -> Vec<PayloadFieldSchema> {
    match field_condition {
        FieldCondition {
            r#match: Some(r#match),
            ..
        } => vec![match r#match {
            Match::Value(match_value) => infer_type_from_match_value(match_value),
            Match::Text(_match_text) => {
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    tokenizer: TokenizerType::default(),
                    min_token_len: None,
                    max_token_len: None,
                    lowercase: None,
                }))
            }
            Match::Any(match_any) => infer_type_from_any_variants(&match_any.any),
            Match::Except(match_except) => infer_type_from_any_variants(&match_except.except),
        }],
        FieldCondition {
            range: Some(_range),
            ..
        } => vec![
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Float),
        ],
        FieldCondition {
            geo_bounding_box: Some(_),
            ..
        }
        | FieldCondition {
            geo_radius: Some(_),
            ..
        }
        | FieldCondition {
            geo_polygon: Some(_),
            ..
        } => vec![PayloadFieldSchema::FieldType(PayloadSchemaType::Geo)],
        FieldCondition {
            key: _,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: _,
        } => vec![],
    }
}

impl TryFrom<(FieldCondition, String)> for UnindexedField {
    type Error = OperationError;

    /// Try to form an issue from a field condition and a collection name
    ///
    /// # Failures
    ///
    /// Will fail if the field condition cannot be used for inferring an appropriate schema.
    /// For example, when there is no index that can be built to improve performance.
    fn try_from(condition_collection_tuple: (FieldCondition, String)) -> Result<Self, Self::Error> {
        let (condition, collection) = condition_collection_tuple;
        let field_schemas = infer_type_from_field_condition(&condition);

        if field_schemas.is_empty() {
            return Err(OperationError::TypeInferenceError {
                field_name: condition.key,
            });
        }

        Ok(Self {
            field_name: condition.key,
            field_schemas,
            collection_name: collection,
        })
    }
}
