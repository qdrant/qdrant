//! BM25 over the text index of a payload field, as a shard runs it.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::query_context::QueryContext;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::index::field_index::full_text_index::tokenizers::Tokenizer;
use segment::json_path::JsonPath;
use segment::types::PayloadSchemaParams;

use crate::payload_index_schema::PayloadIndexSchema;

/// A BM25 query: raw text scored against the text index of one payload field.
///
/// It carries the text, not tokens: each shard tokenizes it with the field's
/// own tokenizer, the one that built the vocabulary the terms are looked up in.
#[derive(Debug, Clone, PartialEq)]
pub struct TextScoringQuery {
    pub field: JsonPath,
    pub text: String,
    pub params: Bm25Params,
}

impl TextScoringQuery {
    /// The query terms, tokenized as the field's text index tokenizes a
    /// query. A field the schema holds no text index for is an error: there
    /// is nothing to score it with.
    pub fn tokenize(&self, schema: &PayloadIndexSchema) -> OperationResult<Vec<String>> {
        let field_schema = schema.schema.get(&self.field).ok_or_else(|| {
            OperationError::validation_error(format!(
                "BM25 requires a text index on field {}, which has none",
                self.field,
            ))
        })?;
        let PayloadSchemaParams::Text(params) = field_schema.expand().into_owned() else {
            return Err(OperationError::validation_error(format!(
                "BM25 requires a text index on field {}, which is indexed as {:?}",
                self.field,
                field_schema.kind(),
            )));
        };
        let tokenizer = Tokenizer::new_from_text_index_params(&params);
        let mut terms = Vec::new();
        tokenizer.tokenize_query(&self.text, |token| terms.push(token.into_owned()));
        Ok(terms)
    }
}

/// A query context seeded with the text statistics of `field` for `terms`,
/// ready to be filled over the shard's segments by
/// [`fill_query_context`](super::query_context::fill_query_context).
pub fn init_text_query_context(
    field: &JsonPath,
    terms: &[String],
    is_stopped: Arc<AtomicBool>,
    hw_measurement_acc: HwMeasurementAcc,
) -> QueryContext {
    // The threshold only picks between plain and indexed vector search.
    let mut query_context =
        QueryContext::new(usize::MAX, hw_measurement_acc).with_is_stopped(is_stopped);
    query_context.init_text_stats(field, terms.iter().cloned());
    query_context
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use segment::data_types::index::{Language, StopwordsInterface, TextIndexParams};
    use segment::types::{PayloadFieldSchema, PayloadSchemaType};

    use super::*;

    fn query(field: &str, text: &str) -> TextScoringQuery {
        TextScoringQuery {
            field: JsonPath::new(field),
            text: text.to_owned(),
            params: Bm25Params::default(),
        }
    }

    fn schema(
        fields: impl IntoIterator<Item = (&'static str, PayloadFieldSchema)>,
    ) -> PayloadIndexSchema {
        PayloadIndexSchema {
            schema: fields
                .into_iter()
                .map(|(field, schema)| (JsonPath::new(field), schema))
                .collect::<HashMap<_, _>>(),
        }
    }

    /// The query goes through the field's own tokenizer, whichever form the
    /// schema states it in: a bare type means the default parameters.
    #[test]
    fn tokenizes_with_the_field_tokenizer() {
        let schema = schema([
            (
                "bare",
                PayloadFieldSchema::FieldType(PayloadSchemaType::Text),
            ),
            (
                "english",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    stopwords: Some(StopwordsInterface::Language(Language::English)),
                    ..TextIndexParams::default()
                })),
            ),
        ]);
        assert_eq!(
            query("bare", "The Quick fox").tokenize(&schema).unwrap(),
            ["the", "quick", "fox"],
        );
        assert_eq!(
            query("english", "The Quick fox").tokenize(&schema).unwrap(),
            ["quick", "fox"],
        );
    }

    #[test]
    fn refuses_a_field_without_a_text_index() {
        let schema = schema([(
            "keyword",
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
        )]);
        assert!(query("keyword", "fox").tokenize(&schema).is_err());
        assert!(query("missing", "fox").tokenize(&schema).is_err());
    }
}
