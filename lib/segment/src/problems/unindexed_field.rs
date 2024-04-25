use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use http::{HeaderMap, Method, Uri};

use issues::{Action, Code, ImmediateSolution, Issue, Solution};
use itertools::Itertools;

use crate::common::operation_error::OperationError;
use crate::data_types::text_index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::json_path::{JsonPathInterface, JsonPathV2};
use crate::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, PayloadFieldSchema,
    PayloadKeyType, PayloadSchemaParams, PayloadSchemaType,
};

#[derive(Debug)]
pub struct UnindexedField {
    field_name: JsonPathV2,
    field_schemas: HashSet<PayloadFieldSchema>,
    collection_name: String,
    endpoint: Uri,
    distinctive: String,
}

impl UnindexedField {
    pub fn slow_search_threshold() -> Duration {
        static SLOW_SEARCH_THRESHOLD: OnceLock<Duration> = OnceLock::new();

        *SLOW_SEARCH_THRESHOLD.get_or_init(|| {
            Duration::from_millis(
                std::env::var("QDRANT_SLOW_SEARCH_THRESHOLD")
                    .ok()
                    .and_then(|var| var.parse::<u64>().ok())
                    .unwrap_or(300),
            )
        })
    }

    pub fn get_distinctive(collection_name: &str, field_name: &JsonPathV2) -> String {
        format!("{collection_name}/{field_name}")
    }

    pub fn get_collection_name(code: &Code) -> &str {
        code.distinctive.split('/').next().unwrap() // Code format is always the same
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

        let distinctive = Self::get_distinctive(&collection_name, &field_name);

        Ok(Self {
            field_name,
            field_schemas,
            collection_name,
            endpoint,
            distinctive,
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
    fn distinctive(&self) -> &str {
        &self.distinctive
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

            ImmediateSolution {
                message: format!(
                    "Create an index on field '{}' of schema '{}' in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
                    self.field_name, serde_json::to_string(&field_schema).unwrap(), self.collection_name
                ),
                action: Action {
                    method: Method::PUT,
                    uri: self.endpoint.clone(),
                    headers: HeaderMap::new(),
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
            // TODO: what to do with these? Any index would suffice
            Condition::IsEmpty(_) | Condition::IsNull(_) => {}

            Condition::HasId(_) => {}
        }
    }
}
