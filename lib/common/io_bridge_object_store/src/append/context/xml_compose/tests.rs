use std::assert_matches;

use object_store::aws::AmazonS3Builder;

use super::super::stub::{SeenRequest, StubResponse, stub_server, stub_store_and_context};
use super::*;

const COMPOSE_BODY: &str = "<ComposeRequest>\
     <Component><Name>dir/append.dat</Name></Component>\
     <Component><Name>dir/append.dat.compose-append</Name></Component>\
     </ComposeRequest>";

/// Append `b"data"` to `dir/append.dat` at `offset` via the stub.
fn append_data_at(endpoint: &str, offset: u64) -> UioResult<u64> {
    let (_, context) = stub_store_and_context(endpoint);
    // As `build_store` configures a GCS endpoint: single-object deletes.
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("id")
            .with_secret_access_key("secret")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_disable_bulk_delete(true)
            .build()
            .unwrap(),
    );
    let key = object_store::path::Path::from("dir/append.dat");

    io_bridge::BridgeRuntime::global().block_on(XmlComposeAppend::new(context).append(
        &store,
        &key,
        offset,
        Bytes::from_static(b"data"),
    ))
}

fn head_ok(size: u64) -> StubResponse {
    StubResponse::new(200)
        .header("content-length", size)
        .header("x-goog-generation", "1794908319617095")
}

fn put_ok() -> StubResponse {
    StubResponse::new(200).header("etag", "\"temp\"")
}

fn delete_ok() -> StubResponse {
    StubResponse::new(204)
}

/// Signed with GOOG4 and free of `x-amz-*` headers, which GCS refuses
/// next to the `x-goog-*` precondition.
fn assert_goog4_signed(request: &SeenRequest) {
    let authorization = request.headers["authorization"].to_str().unwrap();
    assert!(
        authorization.starts_with("GOOG4-HMAC-SHA256 Credential=id/"),
        "{authorization}",
    );
    assert!(
        authorization.contains("/us-east-1/storage/goog4_request, "),
        "{authorization}",
    );
    assert!(request.headers.contains_key("x-goog-date"));
    assert!(request.headers.contains_key("x-goog-content-sha256"));
    for name in request.headers.keys() {
        assert!(
            !name.as_str().starts_with("x-amz-"),
            "{name} on a GOOG4 request"
        );
    }
}

#[test]
fn compose_appends_the_uploaded_tail_conditionally() {
    let (endpoint, seen) = stub_server(vec![
        head_ok(10),
        put_ok(),
        StubResponse::new(200),
        delete_ok(),
    ]);

    assert_eq!(append_data_at(&endpoint, 10).unwrap(), 14);

    let seen = seen.lock().unwrap();
    let [head, upload, compose, delete] = &seen[..] else {
        panic!("expected exactly four requests, got {}", seen.len());
    };

    assert_eq!(head.method, "HEAD");
    assert_eq!(head.path, "/bucket/dir/append.dat");
    assert_goog4_signed(head);

    // The tail goes through the store (and its configured checksum).
    assert_eq!(upload.method, "PUT");
    assert_eq!(upload.path, "/bucket/dir/append.dat.compose-append");
    assert_eq!(upload.body, b"data");
    assert!(upload.signed);

    assert_eq!(compose.method, "PUT");
    assert_eq!(compose.path, "/bucket/dir/append.dat?compose=");
    assert_goog4_signed(compose);
    assert_eq!(
        compose.headers["x-goog-if-generation-match"],
        "1794908319617095",
    );
    assert_eq!(compose.headers["content-type"], "application/xml");
    assert_eq!(String::from_utf8_lossy(&compose.body), COMPOSE_BODY);
    assert_eq!(
        compose.headers[http::header::CONTENT_LENGTH],
        COMPOSE_BODY.len().to_string(),
    );

    assert_eq!(delete.method, "DELETE");
    assert_eq!(delete.path, "/bucket/dir/append.dat.compose-append");
}

/// The object is not at `offset`: nothing is uploaded.
#[test]
fn size_mismatch_is_an_offset_conflict() {
    let (endpoint, seen) = stub_server(vec![head_ok(9)]);

    let err = append_data_at(&endpoint, 10).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 10, .. }
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// The generation moved between the HEAD and the compose: an offset
/// conflict, and the temporary object is still removed.
#[test]
fn precondition_failure_is_an_offset_conflict() {
    let (endpoint, seen) = stub_server(vec![
        head_ok(10),
        put_ok(),
        StubResponse::new(412),
        delete_ok(),
    ]);

    let err = append_data_at(&endpoint, 10).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 10, .. }
    );

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 4);
    assert_eq!(seen[3].method, "DELETE");
}
