use common::types::ScoreType;
use segment::data_types::load_profile::LoadProfile;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use shard::query::ScoringQuery;
use shard::query::query_enum::QueryEnum;

/// Universal query over an edge shard: a scoring query with optional prefetch stages.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRequest {
    /// Sub-requests resolved first; their results form the candidate set the top-level
    /// `query` re-scores.
    pub prefetches: Vec<Prefetch>,
    /// How to score the candidates. `None` scrolls by id instead of scoring.
    pub query: Option<ScoringQuery>,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Exclude results with a worse score than this.
    pub score_threshold: Option<ScoreType>,
    /// Max number of results to return.
    pub limit: usize,
    /// Offset of the first result to return. May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: usize,
    /// Search params for when there is no prefetch.
    pub params: Option<SearchParams>,
    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: WithVector,
    /// Select which payload to return with the response. Default is false.
    pub with_payload: WithPayloadInterface,
}

impl QueryRequest {
    pub fn new(limit: usize) -> Self {
        Self {
            prefetches: Vec::new(),
            query: None,
            filter: None,
            score_threshold: None,
            limit,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(false),
            with_payload: WithPayloadInterface::Bool(false),
        }
    }

    /// Request-specific [`LoadProfile`] for opening a read-only shard to serve exactly
    /// this query: the union of the profiles of its stages, the root and every prefetch,
    /// each warming what its scoring and filter read. Only the root returns payloads.
    pub fn load_profile(&self) -> LoadProfile {
        let mut profile = stage_load_profile(
            self.query.as_ref(),
            self.filter.as_ref(),
            self.params.as_ref(),
            !self.prefetches.is_empty(),
            self.with_payload.is_required(),
        );
        for prefetch in &self.prefetches {
            prefetch.merge_load_profile(&mut profile);
        }
        profile
    }
}

/// One prefetch stage of a [`QueryRequest`]: produces the candidate set its parent re-scores.
/// Prefetches nest, forming a candidate-resolution tree evaluated leaves-first.
#[derive(Clone, Debug, PartialEq)]
pub struct Prefetch {
    /// Nested sub-prefetches resolved before this one.
    pub prefetches: Vec<Prefetch>,
    /// How to score this stage's candidates. `None` scrolls by id instead of scoring.
    pub query: Option<ScoringQuery>,
    /// Max number of candidates this stage passes to its parent.
    pub limit: usize,
    /// Additional search params.
    pub params: Option<SearchParams>,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Exclude candidates with a worse score than this.
    pub score_threshold: Option<ScoreType>,
}

impl Prefetch {
    pub fn new(limit: usize) -> Self {
        Self {
            prefetches: Vec::new(),
            query: None,
            limit,
            params: None,
            filter: None,
            score_threshold: None,
        }
    }

    /// Fold the profiles of this stage and its nested prefetches into `profile`.
    fn merge_load_profile(&self, profile: &mut LoadProfile) {
        profile.merge(stage_load_profile(
            self.query.as_ref(),
            self.filter.as_ref(),
            self.params.as_ref(),
            !self.prefetches.is_empty(),
            false,
        ));
        for prefetch in &self.prefetches {
            prefetch.merge_load_profile(profile);
        }
    }
}

/// Profile of one query stage, prefetches excluded. A stage with prefetches
/// rescores their candidates, reading a vector per candidate: that vector
/// serves fine cold, like the stored vectors of the results. Unless it is
/// sparse: a sparse query gathers its idf statistics over the whole vector
/// index of every segment, rescoring or not.
fn stage_load_profile(
    query: Option<&ScoringQuery>,
    filter: Option<&Filter>,
    params: Option<&SearchParams>,
    rescores: bool,
    with_payload: bool,
) -> LoadProfile {
    let mut profile = match query {
        Some(ScoringQuery::Vector(query)) if !rescores || is_sparse(query) => {
            LoadProfile::for_search(query.get_vector_name(), filter, with_payload)
        }
        Some(ScoringQuery::Mmr(mmr)) if !rescores => {
            LoadProfile::for_search(&mmr.using, filter, with_payload)
        }
        Some(ScoringQuery::Vector(_) | ScoringQuery::Mmr(_)) => {
            LoadProfile::for_scroll(filter, None, with_payload)
        }
        // BM25 reads the text index of the field, which stays warm like an ordering key.
        Some(ScoringQuery::Text(text)) => {
            LoadProfile::for_scroll(filter, Some(&text.field), with_payload)
        }
        Some(ScoringQuery::OrderBy(order_by)) => {
            LoadProfile::for_scroll(filter, Some(&order_by.key), with_payload)
        }
        // A formula reads its variables and evaluates its conditions per candidate, from
        // the payload indexes or the payload storage.
        Some(ScoringQuery::Formula(formula)) => {
            let conditions = Filter {
                should: Some(formula.conditions.clone()),
                ..Filter::new()
            };
            let mut profile = LoadProfile::for_scroll(filter, None, true);
            profile.merge(LoadProfile::for_scroll(Some(&conditions), None, true));
            for var in &formula.payload_vars {
                profile.merge(LoadProfile::for_scroll(None, Some(var), true));
            }
            profile
        }
        Some(ScoringQuery::Fusion(_) | ScoringQuery::Sample(_)) | None => {
            LoadProfile::for_scroll(filter, None, with_payload)
        }
    };
    if query.is_some_and(scores_with_idf)
        && let Some(corpus) = params
            .and_then(|params| params.idf.as_ref())
            .and_then(|idf| idf.corpus())
    {
        profile.merge(LoadProfile::for_scroll(Some(corpus), None, false));
    }
    profile
}

/// Whether the stage gathers idf statistics (sparse or BM25) over its own corpus
/// filter. The other stages ignore the params.
fn scores_with_idf(query: &ScoringQuery) -> bool {
    match query {
        ScoringQuery::Vector(_) | ScoringQuery::Mmr(_) | ScoringQuery::Text(_) => true,
        ScoringQuery::Fusion(_)
        | ScoringQuery::OrderBy(_)
        | ScoringQuery::Formula(_)
        | ScoringQuery::Sample(_) => false,
    }
}

fn is_sparse(query: &QueryEnum) -> bool {
    let mut sparse = false;
    query.iterate_sparse(|_, _| sparse = true);
    sparse
}

#[cfg(test)]
mod tests {
    use common::universal_io::Populate;
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::index::field_index::full_text_index::Bm25Params;
    use segment::index::query_optimization::rescore_formula::parsed_formula::{
        ParsedExpression, ParsedFormula, VariableId,
    };
    use segment::json_path::JsonPath;
    use segment::types::{
        Condition, FieldCondition, IdfCorpusParams, IdfParams, Match, ValueVariants,
    };
    use shard::query::FusionInternal;
    use shard::query::text::TextScoringQuery;
    use sparse::common::sparse_vector::SparseVector;

    use super::*;

    fn path(raw: &str) -> JsonPath {
        raw.parse().unwrap()
    }

    fn match_filter(key: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            path(key),
            Match::new_value(ValueVariants::String("value".to_string())),
        )))
    }

    /// A text leaf in a prefetch warms its field, the prefetch's filter and the
    /// fields of its idf corpus; nothing else.
    #[test]
    fn text_prefetch_warms_its_field_filter_and_corpus() {
        let text = ScoringQuery::Text(TextScoringQuery {
            field: path("body"),
            text: "alpha".to_string(),
            params: Bm25Params::default(),
        });
        let request = QueryRequest {
            prefetches: vec![Prefetch {
                query: Some(text),
                filter: Some(match_filter("city")),
                params: Some(SearchParams {
                    idf: Some(IdfParams::Corpus(IdfCorpusParams {
                        corpus: match_filter("lang"),
                    })),
                    ..Default::default()
                }),
                ..Prefetch::new(10)
            }],
            query: Some(ScoringQuery::Fusion(FusionInternal::Rrf {
                k: 60,
                weights: None,
            })),
            ..QueryRequest::new(10)
        };

        let profile = request.load_profile();
        for warm in ["body", "city", "lang"] {
            assert_eq!(profile.payload_index_placement(&path(warm)), None, "{warm}");
        }
        assert_eq!(
            profile.payload_index_placement(&path("other")),
            Some(Populate::No),
        );
        assert_eq!(profile.vector_storage_placement(""), Some(Populate::No));
    }

    fn nearest(using: &str) -> ScoringQuery {
        ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::from(vec![1.0]),
            using.to_string(),
        )))
    }

    fn text(field: &str) -> ScoringQuery {
        ScoringQuery::Text(TextScoringQuery {
            field: path(field),
            text: "alpha".to_string(),
            params: Bm25Params::default(),
        })
    }

    /// A root that rescores prefetched candidates leaves its vector cold; the
    /// prefetch searching another vector warms that one.
    #[test]
    fn rescoring_root_leaves_its_vector_cold() {
        let request = QueryRequest {
            prefetches: vec![Prefetch {
                query: Some(nearest("small")),
                ..Prefetch::new(100)
            }],
            query: Some(nearest("large")),
            ..QueryRequest::new(10)
        };

        let profile = request.load_profile();
        assert_eq!(profile.vector_storage_placement("small"), None);
        assert_eq!(
            profile.vector_storage_placement("large"),
            Some(Populate::No),
        );
    }

    /// A sparse root keeps its vector warm even when it only rescores: its idf
    /// statistics read the whole sparse index.
    #[test]
    fn rescoring_sparse_root_warms_its_vector() {
        let sparse = SparseVector::new(vec![1, 2], vec![0.5, 0.5]).unwrap();
        let request = QueryRequest {
            prefetches: vec![Prefetch {
                query: Some(nearest("dense")),
                ..Prefetch::new(100)
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(sparse),
                "sparse".to_string(),
            )))),
            ..QueryRequest::new(10)
        };

        let profile = request.load_profile();
        assert_eq!(profile.vector_storage_placement("sparse"), None);
        assert_eq!(profile.vector_index_placement("sparse"), None);
    }

    /// Prefetches nested in prefetches contribute their own profile.
    #[test]
    fn nested_prefetch_warms_its_field() {
        let request = QueryRequest {
            prefetches: vec![Prefetch {
                prefetches: vec![Prefetch {
                    query: Some(text("body")),
                    filter: Some(match_filter("city")),
                    ..Prefetch::new(100)
                }],
                query: Some(nearest("dense")),
                ..Prefetch::new(10)
            }],
            ..QueryRequest::new(10)
        };

        let profile = request.load_profile();
        for warm in ["body", "city"] {
            assert_eq!(profile.payload_index_placement(&path(warm)), None, "{warm}");
        }
        // The middle stage rescores, so its dense vector stays cold.
        assert_eq!(
            profile.vector_storage_placement("dense"),
            Some(Populate::No),
        );
    }

    /// A formula warms its variables, the fields of its conditions and the
    /// payload storage.
    #[test]
    fn formula_warms_its_variables_conditions_and_payload() {
        let formula = ParsedFormula {
            payload_vars: [path("price")].into(),
            conditions: vec![Condition::Field(FieldCondition::new_match(
                path("city"),
                Match::new_value(ValueVariants::String("value".to_string())),
            ))],
            defaults: Default::default(),
            formula: ParsedExpression::Variable(VariableId::Score(0)),
        };
        let request = QueryRequest {
            prefetches: vec![Prefetch {
                query: Some(nearest("dense")),
                ..Prefetch::new(100)
            }],
            query: Some(ScoringQuery::Formula(formula)),
            ..QueryRequest::new(10)
        };

        let profile = request.load_profile();
        for warm in ["price", "city"] {
            assert_eq!(profile.payload_index_placement(&path(warm)), None, "{warm}");
        }
        assert_eq!(
            profile.payload_index_placement(&path("other")),
            Some(Populate::No),
        );
        assert_eq!(profile.payload_storage_placement(), None);
    }
}
