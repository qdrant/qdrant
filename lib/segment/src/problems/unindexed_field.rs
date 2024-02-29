use std::collections::HashMap;
use std::time::Duration;

use http::{HeaderMap, Method, Uri};
use issues::{Action, CodeType, ImmediateSolution, Issue, Solution};
use itertools::Itertools;

use crate::common::operation_error::OperationError;
use crate::common::utils::JsonPathPayload;
use crate::data_types::text_index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, PayloadFieldSchema,
    PayloadIndexInfo, PayloadKeyType, PayloadSchemaParams, PayloadSchemaType,
};

pub struct UnindexedField {
    field_name: String,
    field_schemas: Vec<PayloadFieldSchema>,
    collection_name: String,
}

impl UnindexedField {
    pub const SLOW_SEARCH_THRESHOLD: Duration = Duration::from_millis(300);

    pub fn get_code(collection_name: &str, field_name: &str) -> CodeType {
        format!("{collection_name}/UNINDEXED_FIELD/{field_name}")
    }

    /// Try to form an issue from a field condition and a collection name
    ///
    /// # Errors
    ///
    /// Will fail if the field condition cannot be used for inferring an appropriate schema.
    /// For example, when there is no index that can be built to improve performance.
    pub fn try_new(
        condition: FieldCondition,
        collection_name: String,
    ) -> Result<Self, OperationError> {
        let field_schemas = infer_schema_from_field_condition(&condition);

        if field_schemas.is_empty() {
            return Err(OperationError::TypeInferenceError {
                field_name: condition.key,
            });
        }

        Ok(Self {
            field_name: condition.key,
            field_schemas,
            collection_name,
        })
    }

    pub fn submit_possible_suspects(
        filter: &Filter,
        payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
        collection_name: String,
    ) {
        let unindexed_issues =
            Extractor::new(filter, payload_schema, collection_name).into_issues();
        for issue in unindexed_issues {
            issue.submit();
        }
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
            })
            .as_object()
            .unwrap()
            .clone();

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

fn infer_schema_from_match_value(value: &MatchValue) -> PayloadFieldSchema {
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

fn infer_schema_from_any_variants(value: &AnyVariants) -> PayloadFieldSchema {
    match value {
        AnyVariants::Keywords(_strings) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)
        }
        AnyVariants::Integers(_integers) => {
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)
        }
    }
}

fn infer_schema_from_field_condition(field_condition: &FieldCondition) -> Vec<PayloadFieldSchema> {
    match field_condition {
        FieldCondition {
            r#match: Some(r#match),
            ..
        } => vec![match r#match {
            Match::Value(match_value) => infer_schema_from_match_value(match_value),
            Match::Text(_match_text) => {
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    tokenizer: TokenizerType::default(),
                    min_token_len: None,
                    max_token_len: None,
                    lowercase: None,
                }))
            }
            Match::Any(match_any) => infer_schema_from_any_variants(&match_any.any),
            Match::Except(match_except) => infer_schema_from_any_variants(&match_except.except),
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

struct Extractor {
    payload_schema: HashMap<PayloadKeyType, PayloadFieldSchema>,
    unindexed_schema: HashMap<PayloadKeyType, Vec<PayloadFieldSchema>>,
    collection_name: String,
}

impl Extractor {
    fn new(
        filter: &Filter,
        payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
        collection_name: String,
    ) -> Self {
        // Turn PayloadIndexInfo into PayloadFieldSchema
        let payload_schema = payload_schema
            .into_iter()
            .filter_map(|(key, value)| value.try_into().map(|schema| (key, schema)).ok())
            .collect();

        let mut extractor = Self {
            payload_schema,
            unindexed_schema: HashMap::new(),
            collection_name,
        };

        extractor.update_from_filter(None, filter);

        extractor
    }

    fn into_issues(self) -> Vec<UnindexedField> {
        self.unindexed_schema
            .into_iter()
            .map(|(key, mut field_schemas)| {
                field_schemas.dedup();

                UnindexedField {
                    field_name: key,
                    field_schemas,
                    collection_name: self.collection_name.clone(),
                }
            })
            .collect()
    }

    fn update_from_filter(&mut self, nested_prefix: Option<&JsonPathPayload>, filter: &Filter) {
        let Filter {
            must,
            should,
            min_should,
            must_not,
        } = filter;

        let min_should = min_should.as_ref().map(|min_should| &min_should.conditions);

        [
            must.as_ref(),
            should.as_ref(),
            min_should,
            must_not.as_ref(),
        ]
        .into_iter()
        .flatten()
        .flatten()
        .for_each(|condition| self.update_from_condition(nested_prefix, condition));
    }

    fn update_from_condition(
        &mut self,
        nested_prefix: Option<&JsonPathPayload>,
        condition: &Condition,
    ) {
        match condition {
            Condition::Field(field_condition) => {
                let full_key = JsonPathPayload::extend_or_new(nested_prefix, &field_condition.key);

                let inferred = infer_schema_from_field_condition(field_condition);

                let mut needs_index = false;
                match self.payload_schema.get(&full_key.path) {
                    Some(index_info) => {
                        let already_indexed =
                            inferred.iter().any(|inferred| inferred == index_info);

                        if !already_indexed {
                            needs_index = true;
                        }
                    }
                    None => {
                        needs_index = true;
                    }
                }

                if needs_index {
                    self.unindexed_schema
                        .entry(full_key.path)
                        .and_modify(|entry| entry.extend(inferred))
                        .or_default();
                }
            }
            Condition::Filter(filter) => {
                self.update_from_filter(nested_prefix, filter);
            }
            Condition::Nested(nested) => self.update_from_filter(
                Some(&JsonPathPayload::extend_or_new(
                    nested_prefix,
                    nested.raw_key(),
                )),
                nested.filter(),
            ),
            // TODO: what to do with these? Any index would suffice
            Condition::IsEmpty(_) | Condition::IsNull(_) => {}

            Condition::HasId(_) => {}
        }
    }
}
