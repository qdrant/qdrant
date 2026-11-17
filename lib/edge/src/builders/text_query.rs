//! Fluent builder for a text [`ScoringQuery`].

use segment::index::field_index::full_text_index::Bm25Params as SegmentBm25Params;
use segment::json_path::JsonPath;
use shard::query::ScoringQuery;
use shard::query::text::TextScoringQuery;

use crate::types::TextQueryScoring;

/// Fluent builder for [`ScoringQuery::Text`]: rank points by BM25 over the
/// text index of a payload field. The index must be created with `scoring`.
///
/// `field` and `query` are required and passed through [`Self::new`]; without
/// [`Self::scoring`], the field's scorer runs with its defaults.
#[derive(Clone, Debug)]
pub struct TextQueryBuilder {
    field: JsonPath,
    query: String,
    scoring: Option<TextQueryScoring>,
}

impl TextQueryBuilder {
    /// `query` is tokenized by the field's text index.
    pub fn new(field: JsonPath, query: impl Into<String>) -> Self {
        Self {
            field,
            query: query.into(),
            scoring: None,
        }
    }

    pub fn scoring(mut self, scoring: TextQueryScoring) -> Self {
        self.scoring = Some(scoring);
        self
    }

    pub fn build(self) -> ScoringQuery {
        let Self {
            field,
            query,
            scoring,
        } = self;
        let params = match scoring {
            None => SegmentBm25Params::default(),
            Some(TextQueryScoring::Bm25(params)) => SegmentBm25Params::from(params),
        };
        ScoringQuery::Text(TextScoringQuery {
            field,
            text: query,
            params,
        })
    }
}
