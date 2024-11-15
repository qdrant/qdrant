use std::collections::HashSet;

use api::grpc::qdrant::vector_input::Variant;
use api::grpc::qdrant::{
    query, ContextInput, ContextInputPair, DiscoverInput, PrefetchQuery, Query, RecommendInput,
    VectorInput,
};
use api::rest::schema as rest;
use tonic::Status;

use super::service::{InferenceData, InferenceInput, InferenceRequest};

pub struct BatchAccumGrpc {
    pub(crate) objects: HashSet<InferenceData>,
}

impl BatchAccumGrpc {
    pub fn new() -> Self {
        Self {
            objects: HashSet::new(),
        }
    }

    pub fn add(&mut self, data: InferenceData) {
        self.objects.insert(data);
    }

    pub fn extend(&mut self, other: BatchAccumGrpc) {
        self.objects.extend(other.objects);
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

impl From<&BatchAccumGrpc> for InferenceRequest {
    fn from(batch: &BatchAccumGrpc) -> Self {
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

fn collect_vector_input(vector: &VectorInput, batch: &mut BatchAccumGrpc) -> Result<(), Status> {
    let Some(variant) = &vector.variant else {
        return Ok(());
    };

    match variant {
        Variant::Id(_) => {}
        Variant::Dense(_) => {}
        Variant::Sparse(_) => {}
        Variant::MultiDense(_) => {}
        Variant::Document(document) => {
            let doc = rest::Document::try_from(document.clone())
                .map_err(|e| Status::internal(format!("Document conversion error: {e:?}")))?;
            batch.add(InferenceData::Document(doc));
        }
        Variant::Image(image) => {
            let img = rest::Image::try_from(image.clone())
                .map_err(|e| Status::internal(format!("Image conversion error: {e:?}")))?;
            batch.add(InferenceData::Image(img));
        }
        Variant::Object(object) => {
            let obj = rest::InferenceObject::try_from(object.clone())
                .map_err(|e| Status::internal(format!("Object conversion error: {e:?}")))?;
            batch.add(InferenceData::Object(obj));
        }
    }
    Ok(())
}

pub(crate) fn collect_context_input(
    context: &ContextInput,
    batch: &mut BatchAccumGrpc,
) -> Result<(), Status> {
    let ContextInput { pairs } = context;

    for pair in pairs {
        collect_context_input_pair(pair, batch)?;
    }

    Ok(())
}

fn collect_context_input_pair(
    pair: &ContextInputPair,
    batch: &mut BatchAccumGrpc,
) -> Result<(), Status> {
    let ContextInputPair { positive, negative } = pair;

    if let Some(positive) = positive {
        collect_vector_input(positive, batch)?;
    }

    if let Some(negative) = negative {
        collect_vector_input(negative, batch)?;
    }

    Ok(())
}

pub(crate) fn collect_discover_input(
    discover: &DiscoverInput,
    batch: &mut BatchAccumGrpc,
) -> Result<(), Status> {
    let DiscoverInput { target, context } = discover;

    if let Some(vector) = target {
        collect_vector_input(vector, batch)?;
    }

    if let Some(context) = context {
        for pair in &context.pairs {
            collect_context_input_pair(pair, batch)?;
        }
    }

    Ok(())
}

pub(crate) fn collect_recommend_input(
    recommend: &RecommendInput,
    batch: &mut BatchAccumGrpc,
) -> Result<(), Status> {
    let RecommendInput {
        positive,
        negative,
        strategy: _,
    } = recommend;

    for vector in positive {
        collect_vector_input(vector, batch)?;
    }

    for vector in negative {
        collect_vector_input(vector, batch)?;
    }

    Ok(())
}

pub(crate) fn collect_query(query: &Query, batch: &mut BatchAccumGrpc) -> Result<(), Status> {
    let Some(variant) = &query.variant else {
        return Ok(());
    };

    match variant {
        query::Variant::Nearest(nearest) => collect_vector_input(nearest, batch)?,
        query::Variant::Recommend(recommend) => collect_recommend_input(recommend, batch)?,
        query::Variant::Discover(discover) => collect_discover_input(discover, batch)?,
        query::Variant::Context(context) => collect_context_input(context, batch)?,
        query::Variant::OrderBy(_) => {}
        query::Variant::Fusion(_) => {}
        query::Variant::Sample(_) => {}
    }

    Ok(())
}

pub(crate) fn collect_prefetch(
    prefetch: &PrefetchQuery,
    batch: &mut BatchAccumGrpc,
) -> Result<(), Status> {
    let PrefetchQuery {
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
        collect_query(query, batch)?;
    }

    for p in prefetch {
        collect_prefetch(p, batch)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::rest::schema::{Document, Image, InferenceObject};
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
        let mut batch = BatchAccumGrpc::new();
        assert!(batch.objects.is_empty());

        let doc = InferenceData::Document(create_test_document("test"));
        batch.add(doc.clone());
        assert_eq!(batch.objects.len(), 1);

        batch.add(doc);
        assert_eq!(batch.objects.len(), 1);
    }

    #[test]
    fn test_batch_accum_extend() {
        let mut batch1 = BatchAccumGrpc::new();
        let mut batch2 = BatchAccumGrpc::new();

        let doc1 = InferenceData::Document(create_test_document("test1"));
        let doc2 = InferenceData::Document(create_test_document("test2"));

        batch1.add(doc1);
        batch2.add(doc2);

        batch1.extend(batch2);
        assert_eq!(batch1.objects.len(), 2);
    }

    #[test]
    fn test_deduplication() {
        let mut batch = BatchAccumGrpc::new();

        let doc1 = InferenceData::Document(create_test_document("same"));
        let doc2 = InferenceData::Document(create_test_document("same"));

        batch.add(doc1);
        batch.add(doc2);

        assert_eq!(batch.objects.len(), 1);
    }

    #[test]
    fn test_different_model_same_content() {
        let mut batch = BatchAccumGrpc::new();

        let mut doc1 = create_test_document("same");
        let mut doc2 = create_test_document("same");
        doc1.model = "model1".to_string();
        doc2.model = "model2".to_string();

        batch.add(InferenceData::Document(doc1));
        batch.add(InferenceData::Document(doc2));

        assert_eq!(batch.objects.len(), 2);
    }
}
