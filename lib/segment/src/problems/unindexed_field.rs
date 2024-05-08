use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Uri};
use issues::{Action, Code, ImmediateSolution, Issue, Solution};
use itertools::Itertools;
use strum::IntoEnumIterator as _;

use crate::common::operation_error::OperationError;
use crate::data_types::text_index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::json_path::{JsonPathInterface, JsonPathV2};
use crate::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, PayloadFieldSchema,
    PayloadKeyType, PayloadSchemaParams, PayloadSchemaType, RangeInterface,
};
#[derive(Debug)]
pub struct UnindexedField {
    field_name: JsonPathV2,
    field_schemas: HashSet<PayloadFieldSchema>,
    collection_name: String,
    endpoint: Uri,
    instance_id: String,
}

/// Don't use this directly, use `UnindexedField::slow_query_threshold()` instead
pub static SLOW_QUERY_THRESHOLD: OnceLock<Duration> = OnceLock::new();

impl UnindexedField {
    const DEFAULT_SLOW_QUERY_SECS: f32 = 1.2;

    pub fn slow_query_threshold() -> Duration {
        *SLOW_QUERY_THRESHOLD.get_or_init(|| Duration::from_secs_f32(Self::DEFAULT_SLOW_QUERY_SECS))
    }

    pub fn get_instance_id(collection_name: &str, field_name: &JsonPathV2) -> String {
        format!("{collection_name}/{field_name}")
    }

    pub fn get_collection_name(code: &Code) -> &str {
        debug_assert!(code.issue_type == TypeId::of::<Self>());
        code.instance_id.split('/').next().unwrap_or("") // Code format is always the same
    }

    /// Try to form an issue from a field condition and a collection name
    ///
    /// # Errors
    ///
    /// Will fail if the field condition cannot be used for inferring an appropriate schema.
    /// For example, when there is no index that can be built to improve performance.
    pub fn try_new(
        field_name: JsonPathV2,
        field_schemas: HashSet<PayloadFieldSchema>,
        collection_name: String,
    ) -> Result<Self, OperationError> {
        if field_schemas.is_empty() {
            return Err(OperationError::ValidationError {
                description: "Cannot create issue which won't have a solution".to_string(),
            });
        }

        let endpoint = match Uri::builder()
            .path_and_query(format!("/collections/{}/index", collection_name).as_str())
            .build()
        {
            Ok(uri) => uri,
            Err(e) => {
                log::trace!("Failed to build uri: {e}");
                return Err(OperationError::ValidationError {
                    description: "Bad collection name".to_string(),
                });
            }
        };

        let instance_id = Self::get_instance_id(&collection_name, &field_name);

        Ok(Self {
            field_name,
            field_schemas,
            collection_name,
            endpoint,
            instance_id,
        })
    }

    pub fn submit_possible_suspects(
        filter: &Filter,
        payload_schema: &HashMap<PayloadKeyType, PayloadFieldSchema>,
        collection_name: String,
    ) {
        let unindexed_issues =
            Extractor::new(filter, payload_schema, collection_name).into_issues();

        log::trace!("Found unindexed issues: {unindexed_issues:#?}");

        for issue in unindexed_issues {
            issue.submit();
        }
    }
}

impl Issue for UnindexedField {
    fn instance_id(&self) -> &str {
        &self.instance_id
    }

    fn name() -> &'static str {
        "UNINDEXED_FIELD"
    }

    fn description(&self) -> String {
        format!(
            "Unindexed field '{}' is slowing down queries in collection '{}'",
            self.field_name, self.collection_name
        )
    }

    fn solution(&self) -> Solution {
        let mut solutions = self.field_schemas.iter().cloned().map(|field_schema| {
            let request_body = serde_json::json!({
                "field_name": self.field_name,
                "field_schema": field_schema,
            })
            .as_object()
            .unwrap()
            .clone();

            let headers = HeaderMap::from_iter([
                (CONTENT_TYPE, HeaderValue::from_static("application/json")),
            ]);

            ImmediateSolution {
                message: format!(
                    "Create an index on field '{}' of schema '{}' in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
                    self.field_name, serde_json::to_string(&field_schema).unwrap(), self.collection_name
                ),
                action: Action {
                    method: Method::PUT,
                    uri: self.endpoint.clone(),
                    headers,
                    body: Some(request_body),
                },
            }
        }).collect_vec();

        match solutions.len() {
            0 => unreachable!(
                "Cannot create a solution without a field schema, protected by try_new()"
            ),
            1 => Solution::Immediate(solutions.pop().unwrap()),
            _ => Solution::ImmediateChoice(solutions),
        }
    }
}

/// Suggest any index, let user choose depending on their data type
fn all_indexes() -> impl Iterator<Item = PayloadFieldSchema> {
    PayloadSchemaType::iter().map(PayloadFieldSchema::FieldType)
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
    let FieldCondition {
        key: _key,
        r#match,
        range,
        geo_bounding_box,
        geo_radius,
        geo_polygon,
        values_count,
    } = field_condition;

    let mut inferred = Vec::new();

    if let Some(r#match) = r#match {
        inferred.push(match r#match {
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
        })
    }
    if let Some(range_interface) = range {
        match range_interface {
            RangeInterface::DateTime(_) => {
                inferred.push(PayloadFieldSchema::FieldType(PayloadSchemaType::Datetime));
            }
            RangeInterface::Float(_) => {
                inferred.push(PayloadFieldSchema::FieldType(PayloadSchemaType::Float));
                inferred.push(PayloadFieldSchema::FieldType(PayloadSchemaType::Integer));
            }
        }
    }
    if geo_bounding_box.is_some() || geo_radius.is_some() || geo_polygon.is_some() {
        inferred.push(PayloadFieldSchema::FieldType(PayloadSchemaType::Geo));
    }
    if values_count.is_some() {
        // Any index will do, let user choose depending on their data type
        inferred.extend(all_indexes());
    }

    inferred
}

struct Extractor<'a> {
    payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>,
    unindexed_schema: HashMap<PayloadKeyType, Vec<PayloadFieldSchema>>,
    collection_name: String,
}

impl<'a> Extractor<'a> {
    fn new(
        filter: &Filter,
        payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>,
        collection_name: String,
    ) -> Self {
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
            .filter_map(|(key, field_schemas)| {
                let field_schemas = HashSet::from_iter(field_schemas);

                UnindexedField::try_new(key, field_schemas, self.collection_name.clone()).ok()
            })
            .collect()
    }

    fn update_from_filter(&mut self, nested_prefix: Option<&JsonPathV2>, filter: &Filter) {
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

    fn update_from_condition(&mut self, nested_prefix: Option<&JsonPathV2>, condition: &Condition) {
        match condition {
            Condition::Field(field_condition) => {
                let full_key = JsonPathV2::extend_or_new(nested_prefix, &field_condition.key);

                let inferred = infer_schema_from_field_condition(field_condition);

                let mut needs_index = false;
                match self.payload_schema.get(&full_key) {
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
                        .entry(full_key)
                        .or_default()
                        .extend(inferred);
                }
            }
            Condition::Filter(filter) => {
                self.update_from_filter(nested_prefix, filter);
            }
            Condition::Nested(nested) => self.update_from_filter(
                Some(&JsonPathV2::extend_or_new(nested_prefix, nested.raw_key())),
                nested.filter(),
            ),
            // Any index will suffice
            Condition::IsEmpty(is_empty) => {
                self.unindexed_schema
                    .entry(JsonPathV2::extend_or_new(
                        nested_prefix,
                        &is_empty.is_empty.key,
                    ))
                    .or_default()
                    .extend(all_indexes());
            }
            Condition::IsNull(is_null) => {
                self.unindexed_schema
                    .entry(JsonPathV2::extend_or_new(
                        nested_prefix,
                        &is_null.is_null.key,
                    ))
                    .or_default()
                    .extend(all_indexes());
            }
            Condition::HasId(_) => {}
        }
    }
}
