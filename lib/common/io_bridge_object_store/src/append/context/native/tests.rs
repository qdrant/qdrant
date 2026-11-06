use std::assert_matches;

use object_store::aws::AmazonS3Builder;
use url::Url;

use super::super::stub::{StubResponse, stub_server, stub_store_and_context};
use super::*;

fn checksum_mismatch() -> StubResponse {
    StubResponse::new(400).body(
        "<Error><Code>InvalidRequest</Code><Message>Checksum Type mismatch occurred, \
         expected checksum Type: sha256, actual checksum Type: crc64nvme.</Message></Error>",
    )
}

#[test]
fn append_always_sends_the_crc64nvme_of_the_new_bytes() {
    let (endpoint, seen) = stub_server(vec![success_with_size(9)]);
    assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

    let seen = seen.lock().unwrap();
    let [request] = &seen[..] else {
        panic!("expected one append");
    };
    assert_eq!(request.headers["x-amz-checksum-crc64nvme"], "7a5hQ478J4A=");
    assert!(!request.headers.contains_key("x-amz-checksum-sha256"));
    assert!(
        request.headers["authorization"]
            .to_str()
            .unwrap()
            .contains("x-amz-checksum-crc64nvme")
    );
    assert_eq!(request.write_offset.as_deref(), Some("5"));
    assert_eq!(request.body, b"data");
}

#[test]
fn checksum_mismatch_stops_without_switching_algorithms() {
    let (endpoint, seen) = stub_server(vec![checksum_mismatch()]);
    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert!(err.to_string().contains("Checksum Type mismatch"), "{err}");
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[test]
fn checksum_mismatch_after_an_uncertain_write_does_not_retry_or_reconcile() {
    let (endpoint, seen) = stub_server(vec![StubResponse::new(503), checksum_mismatch()]);
    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert!(err.to_string().contains("Checksum Type mismatch"), "{err}");
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[test]
fn crc64nvme_append_preserves_lost_acknowledgement_recovery() {
    let (endpoint, seen) = stub_server(vec![
        StubResponse::new(503),
        write_offset_conflict(),
        head_with_size(9),
    ]);

    assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].method, "HEAD");
    for request in &seen[..2] {
        assert_eq!(request.headers["x-amz-checksum-crc64nvme"], "7a5hQ478J4A=");
        assert!(!request.headers.contains_key("x-amz-checksum-sha256"));
    }
}

#[test]
fn unrelated_invalid_request_is_not_retried() {
    let (endpoint, seen) = stub_server(vec![
        StubResponse::new(400).body(
            "<Error><Code>InvalidRequest</Code><Message>Invalid bucket configuration</Message></Error>",
        ),
        success_with_size(9),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();

    assert!(
        err.to_string().contains("Invalid bucket configuration"),
        "{err}"
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[test]
fn unbuildable_request_is_an_error_not_a_panic() {
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("id")
            .with_secret_access_key("secret")
            .build()
            .unwrap(),
    );
    let context = SignedRequestContext::new(
        true,
        "bucket".to_string(),
        Url::parse("http://localhost:9000/bucket").unwrap(),
        "us-east-1".to_string(),
        "s3",
    );
    // `url` accepts this; `http` caps URIs at u16::MAX bytes.
    let key = object_store::path::Path::from("k".repeat(70_000));

    let result = io_bridge::BridgeRuntime::global().block_on(NativeAppend::new(context).append(
        &store,
        &key,
        0,
        Bytes::from_static(b"data"),
    ));
    assert!(matches!(result, Err(UniversalIoError::S3Config { .. })));
}

fn append_data_at(endpoint: &str, offset: u64) -> UioResult<u64> {
    let (store, context) = stub_store_and_context(endpoint);
    let key = object_store::path::Path::from("dir/append.dat");

    io_bridge::BridgeRuntime::global().block_on(NativeAppend::new(context).append(
        &store,
        &key,
        offset,
        Bytes::from_static(b"data"),
    ))
}

fn success_with_size(size: u64) -> StubResponse {
    StubResponse::new(200).header(OBJECT_SIZE_HEADER, size)
}

fn write_offset_conflict() -> StubResponse {
    StubResponse::new(400).body("<Error><Code>InvalidWriteOffset</Code></Error>")
}

fn head_with_size(size: u64) -> StubResponse {
    StubResponse::new(200)
        .header("content-length", size)
        .header("last-modified", "Tue, 14 Jul 2026 12:00:00 GMT")
        .header("etag", "\"stub\"")
}

#[test]
fn append_sends_a_signed_write_offset_put_and_returns_the_new_size() {
    let (endpoint, seen) = stub_server(vec![success_with_size(9)]);
    assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

    let seen = seen.lock().unwrap();
    let [request] = &seen[..] else {
        panic!("expected exactly one request");
    };
    assert_eq!(request.method, "PUT");
    assert_eq!(request.path, "/bucket/dir/append.dat");
    assert_eq!(request.write_offset.as_deref(), Some("5"));
    assert!(request.signed);
    assert_eq!(request.body, b"data");
}

#[test]
fn success_with_a_mismatching_size_is_an_error() {
    let (endpoint, _seen) = stub_server(vec![success_with_size(42)]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    assert!(err.to_string().contains("expected 9"), "{err}");
}

#[test]
fn success_with_an_unparseable_size_is_an_error() {
    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(200).header(OBJECT_SIZE_HEADER, "over 9000"),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
}

#[test]
fn success_without_the_size_header_at_offset_zero_is_a_whole_object_write() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(200)]);

    assert_eq!(append_data_at(&endpoint, 0).unwrap(), 4);
}

#[test]
fn success_without_the_size_header_at_a_nonzero_offset_is_an_error() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(200)]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    assert!(err.to_string().contains(OBJECT_SIZE_HEADER), "{err}");
}

#[test]
fn invalid_write_offset_is_a_conflict() {
    let (endpoint, seen) = stub_server(vec![write_offset_conflict()]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 5, .. }
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[test]
fn precondition_failed_is_a_conflict() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(412)]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 5, .. }
    );
}

#[test]
fn too_many_parts_is_rewrite_required() {
    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(400).body("<Error><Code>TooManyParts</Code></Error>"),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::AppendRewriteRequired { .. });
}

#[test]
fn bad_request_without_the_conflict_code_is_not_a_conflict() {
    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(400).body("<Code>MissingHeader</Code>"),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
}

#[test]
fn not_found_at_a_nonzero_offset_is_a_conflict() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(404)]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 5, .. }
    );

    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(404).body("<Error><Code>NoSuchKey</Code></Error>"),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 5, .. }
    );
}

#[test]
fn not_found_for_a_missing_bucket_is_a_hard_error() {
    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(404).body("<Error><Code>NoSuchBucket</Code></Error>"),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    assert!(err.to_string().contains("NoSuchBucket"), "{err}");
}

#[test]
fn not_found_at_offset_zero_is_not_found() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(404)]);

    let err = append_data_at(&endpoint, 0).unwrap_err();
    let UniversalIoError::NotFound { path } = err else {
        panic!("expected NotFound, got {err:?}");
    };
    assert_eq!(path, PathBuf::from("dir/append.dat"));
}

#[test]
fn transient_failures_are_retried_at_the_same_offset() {
    let (endpoint, seen) = stub_server(vec![StubResponse::new(429), success_with_size(9)]);

    assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|request| request.method == "PUT" && request.write_offset.as_deref() == Some("5")),
    );
}

#[test]
fn persistent_server_errors_fail_after_max_attempts() {
    let responses = (0..MAX_ATTEMPTS).map(|_| StubResponse::new(503)).collect();
    let (endpoint, seen) = stub_server(responses);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    assert!(err.to_string().contains("503"), "{err}");
    assert_eq!(seen.lock().unwrap().len(), MAX_ATTEMPTS as usize);
}

#[test]
fn retried_conflict_reconciles_via_head_when_the_tail_landed() {
    let (endpoint, seen) = stub_server(vec![
        StubResponse::new(503),
        write_offset_conflict(),
        head_with_size(9),
    ]);

    assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].method, "HEAD");
}

#[test]
fn retried_conflict_with_a_foreign_head_size_stays_a_conflict() {
    let (endpoint, _seen) = stub_server(vec![
        StubResponse::new(503),
        write_offset_conflict(),
        head_with_size(7),
    ]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 5, .. }
    );
}

#[test]
fn failure_status_surfaces_the_body_excerpt() {
    let (endpoint, _seen) = stub_server(vec![StubResponse::new(403).body("AccessDenied by stub")]);

    let err = append_data_at(&endpoint, 5).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    let message = err.to_string();
    assert!(message.contains("403"), "{message}");
    assert!(message.contains("AccessDenied by stub"), "{message}");
}
