use std::io::{BufRead as _, BufReader, Write as _};
use std::net::TcpListener;

use futures::TryStreamExt as _;
use object_store::aws::AmazonS3;
use object_store::path::Path;
use object_store::{ObjectStore as _, ObjectStoreExt as _};

use super::*;
use crate::BlobBackend as _;
use crate::backends::aws::{AwsConfig, AwsCredentials};

#[test]
fn classifies_by_s3_price_class() {
    let cases = [
        (Method::GET, None, RequestClass::Get),
        (Method::GET, Some("partNumber=1"), RequestClass::Get),
        (Method::HEAD, None, RequestClass::Get),
        (
            Method::GET,
            Some("list-type=2&prefix=a%2F"),
            RequestClass::List,
        ),
        (Method::GET, Some("uploads"), RequestClass::List),
        (Method::PUT, None, RequestClass::Put),
        (
            Method::PUT,
            Some("partNumber=2&uploadId=x"),
            RequestClass::Put,
        ),
        (Method::POST, Some("uploads"), RequestClass::Put),
        (Method::POST, Some("uploadId=x"), RequestClass::Put),
        (Method::POST, Some("delete"), RequestClass::Delete),
        (Method::DELETE, None, RequestClass::Delete),
        (Method::OPTIONS, None, RequestClass::Other),
    ];
    for (method, query, class) in cases {
        assert_eq!(classify(&method, query), class, "{method} ?{query:?}");
    }
}

#[test]
fn a_key_only_matches_whole_query_keys() {
    assert_eq!(
        classify(&Method::GET, Some("x-list-type=2")),
        RequestClass::Get
    );
    assert_eq!(
        classify(&Method::POST, Some("undelete=1")),
        RequestClass::Put
    );
}

/// Serves `responses` in order, one connection each, ignoring request bodies (none are sent).
fn stub_server(responses: Vec<(u16, String)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for (status, body) in responses {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut reader = BufReader::new(&mut stream);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim_end().is_empty() {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status} Stub\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    endpoint
}

fn store(endpoint: String) -> AmazonS3 {
    AmazonS3::build_store(&AwsConfig {
        bucket: "bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some(endpoint),
        s3_express: false,
        native_append: false,
        credentials: AwsCredentials::Static {
            access_key_id: "id".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        },
    })
    .unwrap()
}

fn list_page(key: &str, next: Option<&str>) -> String {
    let (truncated, token) = match next {
        Some(token) => (
            "true",
            format!("<NextContinuationToken>{token}</NextContinuationToken>"),
        ),
        None => ("false", String::new()),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult><Name>bucket</Name><Prefix></Prefix><KeyCount>1</KeyCount>\
         <MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated>{token}\
         <Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>\
         <ETag>\"e\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>\
         </ListBucketResult>"
    )
}

/// The store built by the backend counts each attempt and each list page, not each call.
/// Deltas are lower bounds: other tests in this process share the counters.
#[tokio::test]
async fn store_counts_retries_and_list_pages() {
    let before = http_request_stats();

    let endpoint = stub_server(vec![
        (503, String::new()),
        (200, "hello".to_string()),
        (200, list_page("a", Some("next"))),
        (200, list_page("b", None)),
    ]);
    let store = store(endpoint);

    let got = store
        .get(&Path::from("key"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(got.as_ref(), b"hello");
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 2);

    let after = http_request_stats();
    let delta = |class, status| after.requests(class, status) - before.requests(class, status);
    assert!(delta(RequestClass::Get, RequestStatus::ServerError) >= 1);
    assert!(delta(RequestClass::Get, RequestStatus::Ok) >= 1);
    assert!(delta(RequestClass::List, RequestStatus::Ok) >= 2);
    assert!(after.bytes_received - before.bytes_received >= 5);
}
