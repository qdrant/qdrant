use std::collections::HashSet;

use api::rest::{
    ContextInput, ContextPair, DiscoverInput, Prefetch, Query, QueryGroupsRequestInternal,
    QueryInterface, RecommendInput, VectorInput,
};

use super::service::{InferenceData, InferenceRequest};

pub struct BatchAccum {
    pub(crate) objects: HashSet<InferenceData>,
}

impl BatchAccum {
    pub fn new() -> Self {
        Self {
            objects: HashSet::new(),
        }
    }

    pub fn add(&mut self, data: InferenceData) {
        self.objects.insert(data);
    }

    pub fn extend(&mut self, other: BatchAccum) {
        self.objects.extend(other.objects);
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

impl From<&BatchAccum> for InferenceRequest {
    fn from(batch: &BatchAccum) -> Self {
        Self {
            inputs: batch.objects.iter().cloned().map(Into::into).collect(),
            inference: None,
            token: None,
        }
    }
}

fn collect_vector_input(vector: &VectorInput, batch: &mut BatchAccum) {
    match vector {
        VectorInput::Document(doc) => batch.add(InferenceData::Document(doc.clone())),
        VectorInput::Image(img) => batch.add(InferenceData::Image(img.clone())),
        VectorInput::Object(obj) => batch.add(InferenceData::Object(obj.clone())),
        // types that are not supported in the Inference Service
        VectorInput::DenseVector(_) => {}
        VectorInput::SparseVector(_) => {}
        VectorInput::MultiDenseVector(_) => {}
        VectorInput::Id(_) => {}
    }
}

fn collect_context_pair(pair: &ContextPair, batch: &mut BatchAccum) {
    collect_vector_input(&pair.positive, batch);
    collect_vector_input(&pair.negative, batch);
}

fn collect_discover_input(discover: &DiscoverInput, batch: &mut BatchAccum) {
    collect_vector_input(&discover.target, batch);
    if let Some(context) = &discover.context {
        for pair in context {
            collect_context_pair(pair, batch);
        }
    }
}

fn collect_recommend_input(recommend: &RecommendInput, batch: &mut BatchAccum) {
    if let Some(positive) = &recommend.positive {
        for vector in positive {
            collect_vector_input(vector, batch);
        }
    }
    if let Some(negative) = &recommend.negative {
        for vector in negative {
            collect_vector_input(vector, batch);
        }
    }
}

fn collect_query(query: &Query, batch: &mut BatchAccum) {
    match query {
        Query::Nearest(nearest) => collect_vector_input(&nearest.nearest, batch),
        Query::Recommend(recommend) => collect_recommend_input(&recommend.recommend, batch),
        Query::Discover(discover) => collect_discover_input(&discover.discover, batch),
        Query::Context(context) => {
            if let ContextInput(Some(pairs)) = &context.context {
                for pair in pairs {
                    collect_context_pair(pair, batch);
                }
            }
        }
        Query::OrderBy(_) | Query::Fusion(_) | Query::Sample(_) => {}
    }
}

fn collect_query_interface(query: &QueryInterface, batch: &mut BatchAccum) {
    match query {
        QueryInterface::Nearest(vector) => collect_vector_input(vector, batch),
        QueryInterface::Query(query) => collect_query(query, batch),
    }
}

fn collect_prefetch(prefetch: &Prefetch, batch: &mut BatchAccum) {
    if let Some(query) = &prefetch.query {
        collect_query_interface(query, batch);
    }

    if let Some(prefetches) = &prefetch.prefetch {
        for p in prefetches {
            collect_prefetch(p, batch);
        }
    }
}

pub fn collect_query_groups_request(request: &QueryGroupsRequestInternal) -> BatchAccum {
    let mut batch = BatchAccum::new();

    if let Some(query) = &request.query {
        collect_query_interface(query, &mut batch);
    }

    if let Some(prefetches) = &request.prefetch {
        for prefetch in prefetches {
            collect_prefetch(prefetch, &mut batch);
        }
    }

    batch
}
