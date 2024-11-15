use std::collections::HashSet;

use api::rest::{
    ContextInput, ContextPair, DiscoverInput, Prefetch, Query, QueryGroupsRequestInternal,
    QueryInterface, QueryRequestInternal, RecommendInput, VectorInput,
};

use super::service::{InferenceData, InferenceInput, InferenceRequest};

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
            inputs: batch
                .objects
                .iter()
                .cloned()
                .map(InferenceInput::from)
                .collect(),
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
    let Prefetch {
        prefetch,
        query,
        using: _,
        filter: _,
        params: _,
        score_threshold: _,
        limit: _,
        lookup_from: _,
    } = prefetch;

    if let Some(query) = query {
        collect_query_interface(query, batch);
    }

    if let Some(prefetches) = prefetch {
        for p in prefetches {
            collect_prefetch(p, batch);
        }
    }
}

pub fn collect_query_groups_request(request: &QueryGroupsRequestInternal) -> BatchAccum {
    let mut batch = BatchAccum::new();

    let QueryGroupsRequestInternal {
        query,
        prefetch,
        using: _,
        filter: _,
        params: _,
        score_threshold: _,
        with_vector: _,
        with_payload: _,
        lookup_from: _,
        group_request: _,
    } = request;

    if let Some(query) = query {
        collect_query_interface(query, &mut batch);
    }

    if let Some(prefetches) = prefetch {
        for prefetch in prefetches {
            collect_prefetch(prefetch, &mut batch);
        }
    }

    batch
}

pub fn collect_query_request(request: &QueryRequestInternal) -> BatchAccum {
    let mut batch = BatchAccum::new();

    let QueryRequestInternal {
        prefetch,
        query,
        using: _,
        filter: _,
        score_threshold: _,
        params: _,
        limit: _,
        offset: _,
        with_vector: _,
        with_payload: _,
        lookup_from: _,
    } = request;

    if let Some(query) = query {
        collect_query_interface(query, &mut batch);
    }

    if let Some(prefetches) = prefetch {
        for prefetch in prefetches {
            collect_prefetch(prefetch, &mut batch);
        }
    }

    batch
}

#[cfg(test)]
mod tests {
    use api::rest::schema::{DiscoverQuery, Document, Image, InferenceObject, NearestQuery};
    use api::rest::QueryBaseGroupRequest;
    use serde_json::json;

    use super::*;

    fn create_test_document(text: &str) -> Document {
        Document {
            text: text.to_string(),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    fn create_test_image(url: &str) -> Image {
        Image {
            image: json!({"data": url.to_string()}),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    fn create_test_object(data: &str) -> InferenceObject {
        InferenceObject {
            object: json!({"data": data}),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    #[test]
    fn test_batch_accum_basic() {
        let mut batch = BatchAccum::new();
        assert!(batch.objects.is_empty());

        let doc = InferenceData::Document(create_test_document("test"));
        batch.add(doc.clone());
        assert_eq!(batch.objects.len(), 1);

        batch.add(doc);
        assert_eq!(batch.objects.len(), 1);
    }

    #[test]
    fn test_batch_accum_extend() {
        let mut batch1 = BatchAccum::new();
        let mut batch2 = BatchAccum::new();

        let doc1 = InferenceData::Document(create_test_document("test1"));
        let doc2 = InferenceData::Document(create_test_document("test2"));

        batch1.add(doc1);
        batch2.add(doc2);

        batch1.extend(batch2);
        assert_eq!(batch1.objects.len(), 2);
    }

    #[test]
    fn test_deduplication() {
        let mut batch = BatchAccum::new();

        let doc1 = InferenceData::Document(create_test_document("same"));
        let doc2 = InferenceData::Document(create_test_document("same"));

        batch.add(doc1);
        batch.add(doc2);

        assert_eq!(batch.objects.len(), 1);
    }

    #[test]
    fn test_collect_vector_input() {
        let mut batch = BatchAccum::new();

        let doc_input = VectorInput::Document(create_test_document("test"));
        let img_input = VectorInput::Image(create_test_image("test.jpg"));
        let obj_input = VectorInput::Object(create_test_object("test"));

        collect_vector_input(&doc_input, &mut batch);
        collect_vector_input(&img_input, &mut batch);
        collect_vector_input(&obj_input, &mut batch);

        assert_eq!(batch.objects.len(), 3);
    }

    #[test]
    fn test_collect_prefetch() {
        let prefetch = Prefetch {
            query: Some(QueryInterface::Nearest(VectorInput::Document(
                create_test_document("test"),
            ))),
            prefetch: Some(vec![Prefetch {
                query: Some(QueryInterface::Nearest(VectorInput::Image(
                    create_test_image("nested.jpg"),
                ))),
                prefetch: None,
                using: None,
                filter: None,
                params: None,
                score_threshold: None,
                limit: None,
                lookup_from: None,
            }]),
            using: None,
            filter: None,
            params: None,
            score_threshold: None,
            limit: None,
            lookup_from: None,
        };

        let mut batch = BatchAccum::new();
        collect_prefetch(&prefetch, &mut batch);
        assert_eq!(batch.objects.len(), 2);
    }

    #[test]
    fn test_collect_query_groups_request() {
        let request = QueryGroupsRequestInternal {
            query: Some(QueryInterface::Query(Query::Nearest(NearestQuery {
                nearest: VectorInput::Document(create_test_document("test")),
            }))),
            prefetch: Some(vec![Prefetch {
                query: Some(QueryInterface::Query(Query::Discover(DiscoverQuery {
                    discover: DiscoverInput {
                        target: VectorInput::Image(create_test_image("test.jpg")),
                        context: Some(vec![ContextPair {
                            positive: VectorInput::Document(create_test_document("pos")),
                            negative: VectorInput::Image(create_test_image("neg.jpg")),
                        }]),
                    },
                }))),
                prefetch: None,
                using: None,
                filter: None,
                params: None,
                score_threshold: None,
                limit: None,
                lookup_from: None,
            }]),
            using: None,
            filter: None,
            params: None,
            score_threshold: None,
            with_vector: None,
            with_payload: None,
            lookup_from: None,
            group_request: QueryBaseGroupRequest {
                group_by: "test".parse().unwrap(),
                group_size: None,
                limit: None,
                with_lookup: None,
            },
        };

        let batch = collect_query_groups_request(&request);
        assert_eq!(batch.objects.len(), 4);
    }

    #[test]
    fn test_different_model_same_content() {
        let mut batch = BatchAccum::new();

        let mut doc1 = create_test_document("same");
        let mut doc2 = create_test_document("same");
        doc1.model = "model1".to_string();
        doc2.model = "model2".to_string();

        batch.add(InferenceData::Document(doc1));
        batch.add(InferenceData::Document(doc2));

        assert_eq!(batch.objects.len(), 2);
    }
}
