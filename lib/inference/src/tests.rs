use std::assert_matches;
use std::collections::HashMap;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use reqwest::StatusCode;
use segment::index::field_index::full_text_index::bm25_embedder::{
    Bm25Embedder, Bm25EmbedderConfig, UnknownLanguage,
};
use serde_json::{Value, json};
use shard::operations::point_ops::VectorPersisted;

use crate::*;

const BM25_LOCAL_MODEL_NAME: &str = "bm25";

fn client(address: Option<String>) -> InferenceClient {
    client_with(address, UnknownLanguage::Legacy)
}

fn client_with(address: Option<String>, unknown_bm25_language: UnknownLanguage) -> InferenceClient {
    InferenceClient::new(InferenceClientConfig {
        address,
        timeout: Duration::from_secs(10),
        token: Some(String::default()),
        user_agent: None,
        unknown_bm25_language,
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

#[tokio::test]
async fn sends_token_headers_and_mode() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("POST", "/")
        .match_header("openai-api-key", "secret")
        .match_body(mockito::Matcher::PartialJson(json!({
            "inference": "search",
            "token": "per-request",
            "inputs": [{"data": "q", "data_type": "text", "model": "m", "options": null}],
        })))
        .with_status(200)
        .with_body(r#"{"embeddings":[[1.0]],"usage":{"models":{"m":{"tokens":3}}}}"#)
        .create_async()
        .await;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("openai-api-key", "secret".parse().unwrap());
    let response = client(Some(server.url()))
        .infer(
            vec![text_input("q", "m")],
            InferenceType::Search,
            RequestParams {
                token: Some("per-request".to_string()),
                headers,
                timeout: None,
            },
        )
        .await
        .unwrap();

    mock.assert_async().await;
    assert_eq!(response.embeddings, vec![VectorPersisted::Dense(vec![1.0])]);
    assert_eq!(response.usage.unwrap().models["m"].tokens, 3);
}

#[tokio::test]
async fn rate_limit_carries_retry_after() {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", "/")
        .with_status(429)
        .with_header("retry-after", "7")
        .with_body(r#"{"error":"slow down"}"#)
        .create_async()
        .await;

    let err = client(Some(server.url()))
        .infer(
            vec![text_input("q", "m")],
            InferenceType::Search,
            RequestParams::default(),
        )
        .await
        .unwrap_err();

    let InferenceError::Rejected(rejection) = err else {
        panic!("expected a rejection, got {err:?}");
    };
    assert_eq!(rejection.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(rejection.retry_after, Some(Duration::from_secs(7)));
    assert_eq!(rejection.message(), "slow down");
}

#[tokio::test]
async fn non_json_error_body_is_the_message() {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", "/")
        .with_status(502)
        .with_body("bad gateway")
        .create_async()
        .await;

    let err = client(Some(server.url()))
        .infer(
            vec![text_input("q", "m")],
            InferenceType::Search,
            RequestParams::default(),
        )
        .await
        .unwrap_err();

    let InferenceError::Rejected(rejection) = err else {
        panic!("expected a rejection, got {err:?}");
    };
    assert_eq!(rejection.error, None);
    assert_eq!(rejection.message(), "bad gateway");
}

#[tokio::test]
async fn too_few_embeddings_is_an_error() {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", "/")
        .with_status(200)
        .with_body(r#"{"embeddings":[[1.0]]}"#)
        .create_async()
        .await;

    let err = client(Some(server.url()))
        .infer(
            vec![text_input("a", "m"), text_input("b", "m")],
            InferenceType::Update,
            RequestParams::default(),
        )
        .await
        .unwrap_err();

    assert_matches!(err, InferenceError::InvalidResponse(_));
}

#[tokio::test]
async fn bm25_needs_no_address_but_remote_models_do() {
    let client = client(None);

    let local = client
        .infer(
            vec![text_input("hello world", "qdrant/bm25")],
            InferenceType::Search,
            RequestParams::default(),
        )
        .await
        .unwrap();
    assert_matches!(local.embeddings[..], [VectorPersisted::Sparse(_)]);

    let err = client
        .infer(
            vec![text_input("hello world", "m")],
            InferenceType::Search,
            RequestParams::default(),
        )
        .await
        .unwrap_err();
    assert_matches!(err, InferenceError::NotConfigured);
}

#[tokio::test]
async fn bm25_rejects_images() {
    let err = client(None)
        .infer(
            vec![InferenceInput {
                data_type: InferenceDataType::Image,
                ..text_input("https://example.com/a.png", "bm25")
            }],
            InferenceType::Update,
            RequestParams::default(),
        )
        .await
        .unwrap_err();
    assert_matches!(err, InferenceError::InvalidInput(_));
}

/// An unrecognized BM25 `language` follows the client's policy: `Legacy` embeds without
/// stemming and stopwords, `Reject` fails the request as invalid input.
#[tokio::test]
async fn bm25_unknown_language_follows_the_client_policy() {
    let input = InferenceInput {
        options: Some([("language".to_string(), json!("none"))].into()),
        ..text_input("the running run", BM25_LOCAL_MODEL_NAME)
    };

    let legacy = client_with(None, UnknownLanguage::Legacy)
        .infer(
            vec![input.clone()],
            InferenceType::Update,
            RequestParams::default(),
        )
        .await
        .unwrap();
    assert_matches!(
        legacy.embeddings.as_slice(),
        [VectorPersisted::Sparse(v)] if v.indices.len() == 3
    );

    let err = client_with(None, UnknownLanguage::Reject)
        .infer(vec![input], InferenceType::Update, RequestParams::default())
        .await
        .unwrap_err();
    assert_matches!(err, InferenceError::InvalidInput(ref msg) if msg.contains("none"));
}
