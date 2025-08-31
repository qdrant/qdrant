use segment::data_types::vectors::VectorInternal;
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use serde_json::{Value, json};

use crate::operations::generalizer::placeholders::size_value_placeholder;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryRequest, Mmr, NearestWithMmr, Query, VectorInputInternal,
    VectorQuery,
};

impl Generalizer for CollectionQueryRequest {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let CollectionQueryRequest {
            prefetch,
            query,
            using,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
            lookup_from,
        } = self;

        let mut result = serde_json::json!({
            "using": using,
            "lookup_from": lookup_from,
            "with_payload": with_payload,
            "with_vector": with_vector,
            "params": params,
            "score_threshold": score_threshold,
        });

        let prefetch_values: Vec<_> = prefetch.iter().map(|p| p.generalize(level)).collect();

        if !prefetch_values.is_empty() {
            result["prefetch"] = serde_json::Value::Array(prefetch_values);
        }

        if let Some(query) = query {
            result["query"] = query.generalize(level);
        }

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                result["limit"] = serde_json::json!(limit);
                result["offset"] = serde_json::json!(offset);
            }
            GeneralizationLevel::VectorAndValues => {
                result["limit"] = size_value_placeholder(*limit);
                result["offset"] = size_value_placeholder(*offset);
            }
        }

        result
    }
}

impl Generalizer for CollectionPrefetch {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let CollectionPrefetch {
            prefetch,
            query,
            using,
            filter,
            score_threshold,
            limit,
            params,
            lookup_from,
        } = self;

        let mut result = serde_json::json!({
            "using": using,
            "lookup_from": lookup_from,
            "params": params,
            "score_threshold": score_threshold,
        });

        let prefetch_values: Vec<_> = prefetch.iter().map(|p| p.generalize(level)).collect();

        if !prefetch_values.is_empty() {
            result["prefetch"] = serde_json::Value::Array(prefetch_values);
        }

        if let Some(query) = query {
            result["query"] = query.generalize(level);
        }

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                result["limit"] = serde_json::json!(limit);
            }
            GeneralizationLevel::VectorAndValues => {
                result["limit"] = size_value_placeholder(*limit);
            }
        }

        result
    }
}

impl Generalizer for Query {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            Query::Vector(vector) => vector.generalize(level),
            Query::Fusion(fusion) => serde_json::to_value(fusion).unwrap_or_default(),
            Query::OrderBy(order_by) => serde_json::to_value(order_by).unwrap_or_default(),
            Query::Formula(formula) => match level {
                GeneralizationLevel::OnlyVector => serde_json::to_value(formula).unwrap(),
                GeneralizationLevel::VectorAndValues => {
                    serde_json::json!({
                        "expression": "formula_expression"
                    })
                }
            },
            Query::Sample(sample) => serde_json::to_value(sample).unwrap_or_default(),
        }
    }
}

impl Generalizer for VectorInputInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            VectorInputInternal::Vector(vector) => vector.generalize(level),
            VectorInputInternal::Id(_) => {
                serde_json::json!({
                    "type": "point_id"
                })
            }
        }
    }
}

impl Generalizer for VectorInternal {
    fn generalize(&self, _level: GeneralizationLevel) -> Value {
        match self {
            VectorInternal::Dense(dense) => {
                serde_json::json!({
                    "type": "dense_vector",
                    "len": dense.len()
                })
            }
            VectorInternal::Sparse(sparse) => {
                serde_json::json!({
                    "type": "sparse_vector",
                    "len": size_value_placeholder(sparse.indices.len())
                })
            }
            VectorInternal::MultiDense(multi) => {
                serde_json::json!({
                    "type": "multi_dense_vector",
                    "num_vectors": size_value_placeholder(multi.num_vectors()),
                    "vector_len": multi.dim
                })
            }
        }
    }
}

impl Generalizer for VectorQuery<VectorInputInternal> {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            VectorQuery::Nearest(nearest) => nearest.generalize(level),
            VectorQuery::NearestWithMmr(nearest_with_mmr) => {
                let NearestWithMmr { nearest, mmr } = nearest_with_mmr;

                let mmr_value = match level {
                    GeneralizationLevel::OnlyVector => {
                        serde_json::to_value(mmr).unwrap_or_default()
                    }
                    GeneralizationLevel::VectorAndValues => {
                        let Mmr {
                            diversity: _,
                            candidates_limit,
                        } = mmr;
                        let candidates_limit = candidates_limit.unwrap_or(0);

                        serde_json::json!({
                            "candidates_limit": size_value_placeholder(candidates_limit),
                        })
                    }
                };

                serde_json::json!({
                    "type": "nearest_with_mmr",
                    "nearest": nearest.generalize(level),
                    "mmr": mmr_value
                })
            }
            VectorQuery::RecommendAverageVector(recommend) => {
                json!({
                    "average_vector": recommend.generalize(level)
                })
            }
            VectorQuery::RecommendBestScore(recommend) => {
                json!({
                    "best_score": recommend.generalize(level)
                })
            }
            VectorQuery::RecommendSumScores(recommend) => {
                json!({
                    "sum_scores": recommend.generalize(level)
                })
            }
            VectorQuery::Discover(discovery) => {
                json!({
                    "discovery": discovery.generalize(level)
                })
            }
            VectorQuery::Context(context) => {
                json!({
                    "context": context.generalize(level)
                })
            }
        }
    }
}

impl Generalizer for ContextQuery<VectorInputInternal> {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let ContextQuery { pairs } = self;
        match level {
            GeneralizationLevel::OnlyVector => {
                let pairs: Vec<_> = pairs.iter().map(|p| p.generalize(level)).collect();
                serde_json::json!({
                    "type": "context_query",
                    "pairs": pairs,
                })
            }
            GeneralizationLevel::VectorAndValues => {
                let num_pairs = size_value_placeholder(pairs.len());
                serde_json::json!({
                    "type": "context_query",
                    "num_pairs": num_pairs,
                })
            }
        }
    }
}

impl Generalizer for ContextPair<VectorInputInternal> {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let ContextPair { positive, negative } = self;
        let positive = positive.generalize(level);
        let negative = negative.generalize(level);
        serde_json::json!({
            "positive": positive,
            "negative": negative,
        })
    }
}

impl Generalizer for DiscoveryQuery<VectorInputInternal> {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let DiscoveryQuery { target, pairs } = self;
        let target = target.generalize(level);
        match level {
            GeneralizationLevel::OnlyVector => {
                let pairs: Vec<_> = pairs.iter().map(|p| p.generalize(level)).collect();
                serde_json::json!({
                    "type": "discovery_query",
                    "target": target,
                    "pairs": pairs,
                })
            }
            GeneralizationLevel::VectorAndValues => {
                let num_pairs = size_value_placeholder(pairs.len());
                serde_json::json!({
                    "type": "discovery_query",
                    "target": target,
                    "num_pairs": num_pairs,
                })
            }
        }
    }
}

impl Generalizer for RecoQuery<VectorInputInternal> {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                let RecoQuery {
                    positives,
                    negatives,
                } = self;

                let positives: Vec<_> = positives.iter().map(|v| v.generalize(level)).collect();
                let negatives: Vec<_> = negatives.iter().map(|v| v.generalize(level)).collect();

                serde_json::json!({
                    "type": "reco_query",
                    "positives": positives,
                    "negatives": negatives,
                })
            }
            GeneralizationLevel::VectorAndValues => {
                let RecoQuery {
                    positives,
                    negatives,
                } = self;

                let num_positives = size_value_placeholder(positives.len());
                let num_negatives = size_value_placeholder(negatives.len());

                serde_json::json!({
                    "type": "reco_query",
                    "num_positives": num_positives,
                    "num_negatives": num_negatives,
                })
            }
        }
    }
}
