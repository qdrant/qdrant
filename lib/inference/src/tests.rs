use std::collections::HashMap;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use segment::index::field_index::full_text_index::bm25_embedder::{
    Bm25Embedder, Bm25EmbedderConfig, UnknownLanguage,
};
use serde_json::{Value, json};
use shard::operations::point_ops::VectorPersisted;

use crate::*;

const BM25_LOCAL_MODEL_NAME: &str = "bm25";

fn client(address: Option<String>) -> InferenceClient {
    InferenceClient::new(InferenceClientConfig {
        address,
        timeout: Duration::from_secs(10),
        token: Some(String::default()),
        user_agent: None,
        unknown_bm25_language: UnknownLanguage::Legacy,
    })
    .unwrap()
}

fn text_input(text: &str, model: &str) -> InferenceInput {
    InferenceInput {
        data: Value::String(text.to_string()),
        data_type: InferenceDataType::Text,
        model: model.to_string(),
        options: None,
    }
}

#[tokio::test]
async fn test_bm25_end_to_end() {
    let mut rng = StdRng::seed_from_u64(42);

    // Test without any BM25
    let only_inference_inputs: Vec<_> = (0..rng.random_range(30..100))
        .map(|_| make_normal_inference_input("this is some input", &mut rng))
        .collect();
    let res = run_inference_with_mocked_remote(only_inference_inputs.clone()).await;
    check_inference_response(only_inference_inputs, res);

    // Test with only BM25
    let only_bm25_inputs: Vec<_> = (0..rng.random_range(30..100))
        .map(|_| make_bm25_inference_input("this is some input"))
        .collect();
    let res = run_inference_with_mocked_remote(only_bm25_inputs.clone()).await;
    check_inference_response(only_bm25_inputs, res);

    // Test BM25 and inference mixed.
    let mut inputs: Vec<InferenceInput> = vec![];
    inputs.extend(
        (0..rng.random_range(30..100)).map(|_| make_bm25_inference_input("this is some input")),
    );
    inputs.extend(
        (0..rng.random_range(30..100))
            .map(|_| make_normal_inference_input("this is some input", &mut rng)),
    );
    inputs.shuffle(&mut rng);
    let res = run_inference_with_mocked_remote(inputs.clone()).await;
    check_inference_response(inputs, res);
}

fn make_normal_inference_input(input: &str, rand: &mut StdRng) -> InferenceInput {
    let options = if rand.random_bool(0.3) {
        let mut opts = HashMap::default();
        let value = rand.random_iter::<char>().take(10).collect::<String>(); // Test utf8
        opts.insert("some-key".to_string(), Value::String(value));
        Some(opts)
    } else {
        None
    };

    InferenceInput {
        options,
        ..text_input(input, "anyModel")
    }
}

fn make_bm25_inference_input(input: &str) -> InferenceInput {
    let options: HashMap<String, Value> =
        serde_json::from_value(serde_json::to_value(Bm25EmbedderConfig::default()).unwrap())
            .unwrap();

    InferenceInput {
        options: Some(options),
        ..text_input(input, BM25_LOCAL_MODEL_NAME)
    }
}

fn check_inference_response(inputs: Vec<InferenceInput>, response: InferenceResponse) {
    assert_eq!(inputs.len(), response.embeddings.len());

    for (idx, (input, response)) in inputs.into_iter().zip(response.embeddings).enumerate() {
        if input.model == BM25_LOCAL_MODEL_NAME {
            // Only BM25 returns sparse vectors; the mocked remote returns dense ones.
            let bm25_config = parse_bm25_config(input.options).unwrap();
            let expected = Bm25Embedder::new(bm25_config, UnknownLanguage::Legacy)
                .unwrap()
                .embed_document(input.data.as_str().unwrap());
            assert_eq!(response, VectorPersisted::Sparse(expected));
        } else {
            assert_eq!(response, VectorPersisted::Dense(vec![0.0; idx]));
        }
    }
}

async fn run_inference_with_mocked_remote(
    inference_inputs: Vec<InferenceInput>,
) -> InferenceResponse {
    let mut server = mockito::Server::new_async().await;

    // Dummy dense vectors whose dimension is the input's position, to check the ordering.
    let expected_embeddings: Vec<_> = inference_inputs
        .iter()
        .enumerate()
        .filter(|(_, item)| item.model != BM25_LOCAL_MODEL_NAME)
        .map(|(index, _)| VectorPersisted::Dense(vec![0.0; index]))
        .collect();

    let mock = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "text/json")
        .with_body(
            json!(InferenceResponse {
                embeddings: expected_embeddings,
                usage: None,
            })
            .to_string(),
        )
        .create_async()
        .await;

    let has_remote_inference_items = inference_inputs
        .iter()
        .any(|i| i.model != BM25_LOCAL_MODEL_NAME);

    let res = client(Some(server.url()))
        .infer(
            inference_inputs,
            InferenceType::Update,
            RequestParams::default(),
        )
        .await
        .expect("Failed to do inference");

    // Exactly 1 request if there is any non-bm25 input, 0 if all inputs are bm25.
    mock.expect(usize::from(has_remote_inference_items))
        .assert_async()
        .await;

    res
}
