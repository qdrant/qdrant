use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Uri};
use issues::{Action, Code, ImmediateSolution, Issue, Solution};
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::index::query_optimization::rescore_formula::parsed_formula::VariableId;
use segment::json_path::JsonPath;
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, PayloadFieldSchema,
    PayloadKeyType, PayloadSchemaParams, PayloadSchemaType, RangeInterface, UuidPayloadType,
};
use strum::{EnumIter, IntoEnumIterator as _};

use crate::operations::universal_query::formula::ExpressionInternal;
#[derive(Debug)]
pub struct UnindexedField {
    field_name: JsonPath,
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

    pub fn get_instance_id(collection_name: &str, field_name: &JsonPath) -> String {
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
        field_name: JsonPath,
        field_schemas: HashSet<PayloadFieldSchema>,
        collection_name: String,
    ) -> Result<Self, OperationError> {
        if field_schemas.is_empty() {
            return Err(OperationError::ValidationError {
                description: "Cannot create issue which won't have a solution".to_string(),
            });
        }

        let endpoint = match Uri::builder()
            .path_and_query(format!("/collections/{collection_name}/index").as_str())
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
            IssueExtractor::new(filter, payload_schema, collection_name).into_issues();

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

    fn related_collection(&self) -> Option<String> {
        Some(self.collection_name.clone())
    }

    fn description(&self) -> String {
        format!(
            "Unindexed field '{}' might be slowing queries down in collection '{}'",
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
                    "Create an index on field '{}' of schema {} in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
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
            1 => Solution::Immediate(Box::new(solutions.pop().unwrap())),
            _ => Solution::ImmediateChoice(solutions),
        }
    }
}

/// Suggest any index, let user choose depending on their data type
fn all_indexes() -> impl Iterator<Item = FieldIndexType> {
    FieldIndexType::iter()
}

fn infer_index_from_match_value(value: &MatchValue) -> Vec<FieldIndexType> {
    match &value.value {
        segment::types::ValueVariants::String(string) => {
            let mut inferred = Vec::new();

            if UuidPayloadType::parse_str(string).is_ok() {
                inferred.push(FieldIndexType::UuidMatch)
            }

            inferred.push(FieldIndexType::KeywordMatch);

            inferred
        }
        segment::types::ValueVariants::Integer(_integer) => {
            vec![FieldIndexType::IntMatch]
        }
        segment::types::ValueVariants::Bool(_boolean) => {
            vec![FieldIndexType::BoolMatch]
        }
    }
}

fn infer_index_from_any_variants(value: &AnyVariants) -> Vec<FieldIndexType> {
    match value {
        AnyVariants::Strings(strings) => {
            let mut inferred = Vec::new();

            if strings
                .iter()
                .all(|s| UuidPayloadType::parse_str(s).is_ok())
            {
                inferred.push(FieldIndexType::UuidMatch)
            }

            inferred.push(FieldIndexType::KeywordMatch);

            inferred
        }
        AnyVariants::Integers(_integers) => {
            vec![FieldIndexType::IntMatch]
        }
    }
}

fn infer_index_from_field_condition(field_condition: &FieldCondition) -> Vec<FieldIndexType> {
    let FieldCondition {
        key: _key,
        r#match,
        range,
        geo_bounding_box,
        geo_radius,
        geo_polygon,
        values_count,
        is_empty,
        is_null,
    } = field_condition;

    let mut required_indexes = Vec::new();

    if let Some(r#match) = r#match {
        required_indexes.extend(match r#match {
            Match::Value(match_value) => infer_index_from_match_value(match_value),
            Match::Text(_match_text) => vec![FieldIndexType::Text],
            Match::Any(match_any) => infer_index_from_any_variants(&match_any.any),
            Match::Except(match_except) => infer_index_from_any_variants(&match_except.except),
        })
    }
    if let Some(range_interface) = range {
        match range_interface {
            RangeInterface::DateTime(_) => {
                required_indexes.push(FieldIndexType::DatetimeRange);
            }
            RangeInterface::Float(_) => {
                required_indexes.push(FieldIndexType::FloatRange);
                required_indexes.push(FieldIndexType::IntRange);
            }
        }
    }
    if geo_bounding_box.is_some() || geo_radius.is_some() || geo_polygon.is_some() {
        required_indexes.push(FieldIndexType::Geo);
    }
    if values_count.is_some() || is_empty.is_some() || is_null.is_some() {
        // Any index will do, let user choose depending on their data type
        required_indexes.extend(all_indexes());
    }

    required_indexes
}

pub struct IssueExtractor<'a> {
    extractor: Extractor<'a>,
    collection_name: String,
}

impl<'a> IssueExtractor<'a> {
    pub fn new(
        filter: &Filter,
        payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>,
        collection_name: String,
    ) -> Self {
        let extractor = Extractor::new_eager(filter, payload_schema);

        Self {
            extractor,
            collection_name,
        }
    }

    fn into_issues(self) -> Vec<UnindexedField> {
        self.extractor
            .unindexed_schema
            .into_iter()
            .filter_map(|(key, field_schemas)| {
                let field_schemas: HashSet<_> = field_schemas
                    .iter()
                    .map(PayloadFieldSchema::kind)
                    .filter(|kind| {
                        let is_advanced = matches!(kind, PayloadSchemaType::Uuid);
                        !is_advanced
                    })
                    .map(PayloadFieldSchema::from)
                    .collect();

                UnindexedField::try_new(key, field_schemas, self.collection_name.clone()).ok()
            })
            .collect()
    }
}

pub struct Extractor<'a> {
    payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>,
    unindexed_schema: HashMap<PayloadKeyType, Vec<PayloadFieldSchema>>,
}

impl<'a> Extractor<'a> {
    /// Creates an extractor and eagerly extracts all unindexed fields from the provided filter.
    fn new_eager(
        filter: &Filter,
        payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>,
    ) -> Self {
        let mut extractor = Self {
            payload_schema,
            unindexed_schema: HashMap::new(),
        };

        extractor.update_from_filter(None, filter);

        extractor
    }

    /// Creates a new lazy 'Extractor'. It needs to call some update method to extract unindexed fields.
    pub fn new(payload_schema: &'a HashMap<PayloadKeyType, PayloadFieldSchema>) -> Self {
        Self {
            payload_schema,
            unindexed_schema: HashMap::new(),
        }
    }

    /// Current unindexed schema.
    pub fn unindexed_schema(&self) -> &HashMap<PayloadKeyType, Vec<PayloadFieldSchema>> {
        &self.unindexed_schema
    }

    /// Checks the filter for unindexed fields.
    fn update_from_filter(&mut self, nested_prefix: Option<&JsonPath>, filter: &Filter) {
        for condition in filter.iter_conditions() {
            self.update_from_condition(nested_prefix, condition);
        }
    }

    /// Checks the filter for an unindexed field, stops at the first one found.
    pub fn update_from_filter_once(&mut self, nested_prefix: Option<&JsonPath>, filter: &Filter) {
        for condition in filter.iter_conditions() {
            self.update_from_condition(nested_prefix, condition);
            if !self.unindexed_schema.is_empty() {
                break;
            }
        }
    }

    fn update_from_condition(&mut self, nested_prefix: Option<&JsonPath>, condition: &Condition) {
        let key;
        let required_index;

        match condition {
            Condition::Field(field_condition) => {
                key = &field_condition.key;
                required_index = infer_index_from_field_condition(field_condition);
            }
            Condition::Filter(filter) => {
                self.update_from_filter(nested_prefix, filter);
                return;
            }
            Condition::Nested(nested) => {
                self.update_from_filter(
                    Some(&JsonPath::extend_or_new(nested_prefix, nested.raw_key())),
                    nested.filter(),
                );
                return;
            }
            // Any index will suffice to get the satellite null index
            Condition::IsEmpty(is_empty) => {
                key = &is_empty.is_empty.key;
                required_index = all_indexes().collect();
            }
            Condition::IsNull(is_null) => {
                key = &is_null.is_null.key;
                required_index = all_indexes().collect();
            }
            // No index needed
            Condition::HasId(_) => return,
            Condition::CustomIdChecker(_) => return,
            Condition::HasVector(_) => return,
        };

        let full_key = JsonPath::extend_or_new(nested_prefix, key);

        if self.needs_index(&full_key, &required_index) {
            let schemas = required_index
                .into_iter()
                .map(PayloadSchemaType::from)
                .map(PayloadFieldSchema::FieldType);
            self.unindexed_schema
                .entry(full_key)
                .or_default()
                .extend(schemas);
        }
    }

    fn needs_index(&self, key: &JsonPath, required_indexes: &[FieldIndexType]) -> bool {
        match self.payload_schema.get(key) {
            Some(index_info) => {
                // check if the index present has the right capabilities
                let index_field_types = schema_capabilities(index_info);
                let already_indexed = required_indexes
                    .iter()
                    .any(|required| index_field_types.contains(required));

                !already_indexed
            }
            None => true,
        }
    }

    pub fn update_from_expression(&mut self, expression: &ExpressionInternal) {
        let key;
        let required_index;

        match expression {
            ExpressionInternal::Constant(_) => return,
            ExpressionInternal::Variable(variable) => {
                // check if it is indexed with a numeric index
                let Ok(var) = variable.parse::<VariableId>() else {
                    // If it fails here, it will also fail when parsing.
                    return;
                };

                match var {
                    VariableId::Score(_) => return,
                    VariableId::Payload(json_path) => {
                        key = json_path;
                        required_index = vec![
                            FieldIndexType::IntMatch,
                            FieldIndexType::IntRange,
                            FieldIndexType::FloatRange,
                        ];
                    }
                    VariableId::Condition(_) => return,
                }
            }
            ExpressionInternal::Condition(condition) => {
                self.update_from_condition(None, condition);
                return;
            }
            ExpressionInternal::GeoDistance { origin: _, to } => {
                key = to.clone();
                required_index = vec![FieldIndexType::Geo];
            }
            ExpressionInternal::Datetime(_) => return,
            ExpressionInternal::DatetimeKey(variable) => {
                key = variable.clone();
                required_index = vec![FieldIndexType::DatetimeRange];
            }
            ExpressionInternal::Mult(expression_internals) => {
                for expr in expression_internals {
                    self.update_from_expression(expr);
                }
                return;
            }
            ExpressionInternal::Sum(expression_internals) => {
                for expr in expression_internals {
                    self.update_from_expression(expr);
                }
                return;
            }
            ExpressionInternal::Neg(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Div {
                left,
                right,
                by_zero_default: _,
            } => {
                self.update_from_expression(left);
                self.update_from_expression(right);
                return;
            }
            ExpressionInternal::Sqrt(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Pow { base, exponent } => {
                self.update_from_expression(base);
                self.update_from_expression(exponent);
                return;
            }
            ExpressionInternal::Exp(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Log10(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Ln(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Abs(expression_internal) => {
                self.update_from_expression(expression_internal);
                return;
            }
            ExpressionInternal::Decay {
                kind: _,
                x,
                target,
                midpoint: _,
                scale: _,
            } => {
                self.update_from_expression(x);
                if let Some(t) = target.as_ref() {
                    self.update_from_expression(t)
                };
                return;
            }
        }

        if self.needs_index(&key, &required_index) {
            let schemas = required_index
                .into_iter()
                .map(PayloadSchemaType::from)
                .map(PayloadFieldSchema::FieldType);
            self.unindexed_schema
                .entry(key)
                .or_default()
                .extend(schemas);
        }
    }
}

/// All types of internal indexes
#[derive(Debug, Eq, PartialEq, EnumIter, Hash)]
enum FieldIndexType {
    IntMatch,
    IntRange,
    KeywordMatch,
    FloatRange,
    Text,
    BoolMatch,
    UuidMatch,
    UuidRange,
    DatetimeRange,
    Geo,
}

fn schema_capabilities(value: &PayloadFieldSchema) -> HashSet<FieldIndexType> {
    let mut index_types = HashSet::new();
    match value {
        PayloadFieldSchema::FieldType(payload_schema_type) => match payload_schema_type {
            PayloadSchemaType::Keyword => index_types.insert(FieldIndexType::KeywordMatch),
            PayloadSchemaType::Integer => {
                index_types.insert(FieldIndexType::IntMatch);
                index_types.insert(FieldIndexType::IntRange)
            }
            PayloadSchemaType::Uuid => {
                index_types.insert(FieldIndexType::UuidMatch);
                index_types.insert(FieldIndexType::UuidRange)
            }
            PayloadSchemaType::Bool => index_types.insert(FieldIndexType::BoolMatch),
            PayloadSchemaType::Float => index_types.insert(FieldIndexType::FloatRange),
            PayloadSchemaType::Geo => index_types.insert(FieldIndexType::Geo),
            PayloadSchemaType::Text => index_types.insert(FieldIndexType::Text),
            PayloadSchemaType::Datetime => index_types.insert(FieldIndexType::DatetimeRange),
        },
        PayloadFieldSchema::FieldParams(payload_schema_params) => match payload_schema_params {
            PayloadSchemaParams::Keyword(_) => index_types.insert(FieldIndexType::KeywordMatch),
            PayloadSchemaParams::Integer(integer_index_params) => {
                if integer_index_params.lookup.unwrap_or(true) {
                    index_types.insert(FieldIndexType::IntMatch);
                }
                if integer_index_params.range.unwrap_or(true) {
                    index_types.insert(FieldIndexType::IntRange);
                }
                debug_assert!(
                    !index_types.is_empty(),
                    "lookup or range must be true for Integer payload index",
                );
                // unifying match arm types
                true
            }
            PayloadSchemaParams::Uuid(_) => {
                index_types.insert(FieldIndexType::UuidMatch);
                index_types.insert(FieldIndexType::UuidRange)
            }
            PayloadSchemaParams::Bool(_) => index_types.insert(FieldIndexType::BoolMatch),
            PayloadSchemaParams::Float(_) => index_types.insert(FieldIndexType::FloatRange),
            PayloadSchemaParams::Geo(_) => index_types.insert(FieldIndexType::Geo),
            PayloadSchemaParams::Text(_) => index_types.insert(FieldIndexType::Text),
            PayloadSchemaParams::Datetime(_) => index_types.insert(FieldIndexType::DatetimeRange),
        },
    };
    index_types
}

impl From<FieldIndexType> for PayloadSchemaType {
    fn from(val: FieldIndexType) -> Self {
        match val {
            FieldIndexType::IntMatch => PayloadSchemaType::Integer,
            FieldIndexType::IntRange => PayloadSchemaType::Integer,
            FieldIndexType::KeywordMatch => PayloadSchemaType::Keyword,
            FieldIndexType::FloatRange => PayloadSchemaType::Float,
            FieldIndexType::Text => PayloadSchemaType::Text,
            FieldIndexType::BoolMatch => PayloadSchemaType::Bool,
            FieldIndexType::UuidMatch => PayloadSchemaType::Uuid,
            FieldIndexType::UuidRange => PayloadSchemaType::Uuid,
            FieldIndexType::DatetimeRange => PayloadSchemaType::Datetime,
            FieldIndexType::Geo => PayloadSchemaType::Geo,
        }
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::index::IntegerIndexParams;

    use super::*;

    #[test]
    fn integer_index_capacities() {
        let params = PayloadSchemaParams::Integer(IntegerIndexParams {
            lookup: Some(true),
            range: Some(true),
            ..Default::default()
        });
        let schema = PayloadFieldSchema::FieldParams(params);
        let index_types = schema_capabilities(&schema);
        assert!(index_types.contains(&FieldIndexType::IntMatch));
        assert!(index_types.contains(&FieldIndexType::IntRange));
    }
}
