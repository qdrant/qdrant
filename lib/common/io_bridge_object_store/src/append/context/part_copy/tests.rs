use std::assert_matches;

use super::super::stub::{StubResponse, stub_server, stub_store_and_context};
use super::*;

/// Multipart-rewrite `dir/append.dat` as its `[0, offset)` prefix plus
/// `b"data"` via the stub.
fn rewrite_data_at(endpoint: &str, offset: u64) -> UioResult<u64> {
    rewrite_data_at_if_match(endpoint, offset, None)
}

fn rewrite_data_at_if_match(
    endpoint: &str,
    offset: u64,
    expected_etag: Option<&str>,
) -> UioResult<u64> {
    let (store, context) = stub_store_and_context(endpoint);
    let key = object_store::path::Path::from("dir/append.dat");

    io_bridge::BridgeRuntime::global().block_on(PartCopyAppend::new(context).append(
        &store,
        &key,
        offset,
        Bytes::from_static(b"data"),
        expected_etag,
    ))
}

fn initiate_ok() -> StubResponse {
    StubResponse::new(200).body(
        "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>\
         </InitiateMultipartUploadResult>",
    )
}

fn copy_part_ok() -> StubResponse {
    StubResponse::new(200).body(
        "<CopyPartResult><ETag>\"etag-copy\"</ETag>\
         <ChecksumCRC64NVME>rosUhgp5mIg=</ChecksumCRC64NVME></CopyPartResult>",
    )
}

fn data_part_ok() -> StubResponse {
    StubResponse::new(200).header("etag", "\"etag-data\"")
}

fn complete_ok() -> StubResponse {
    StubResponse::new(200).body("<CompleteMultipartUploadResult/>")
}

#[test]
fn multipart_rewrite_copies_the_prefix_and_completes() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        copy_part_ok(),
        data_part_ok(),
        complete_ok(),
    ]);

    let offset = 10 * 1024 * 1024;
    assert_eq!(rewrite_data_at(&endpoint, offset).unwrap(), offset + 4);

    let seen = seen.lock().unwrap();
    let [initiate, copy, upload, complete] = &seen[..] else {
        panic!("expected exactly four requests");
    };

    assert_eq!(initiate.method, "POST");
    assert_eq!(initiate.path, "/bucket/dir/append.dat?uploads");
    assert!(initiate.signed);
    assert_eq!(initiate.headers["x-amz-checksum-algorithm"], "CRC64NVME");
    assert_eq!(initiate.headers["x-amz-checksum-type"], "FULL_OBJECT");

    assert_eq!(copy.method, "PUT");
    assert_eq!(
        copy.path,
        "/bucket/dir/append.dat?partNumber=1&uploadId=upload-1"
    );
    assert_eq!(copy.copy_source.as_deref(), Some("/bucket/dir/append.dat"));
    assert_eq!(copy.copy_range.as_deref(), Some("bytes=0-10485759"));

    assert_eq!(upload.method, "PUT");
    assert_eq!(
        upload.path,
        "/bucket/dir/append.dat?partNumber=2&uploadId=upload-1"
    );
    assert_eq!(upload.body, b"data");
    assert_eq!(upload.headers["x-amz-checksum-crc64nvme"], "7a5hQ478J4A=");
    assert!(!upload.headers.contains_key("x-amz-checksum-sha256"));

    assert_eq!(complete.method, "POST");
    assert_eq!(complete.path, "/bucket/dir/append.dat?uploadId=upload-1");
    assert_eq!(
        String::from_utf8_lossy(&complete.body),
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>\"etag-copy\"</ETag>\
         <ChecksumCRC64NVME>rosUhgp5mIg=</ChecksumCRC64NVME></Part>\
         <Part><PartNumber>2</PartNumber><ETag>\"etag-data\"</ETag>\
         <ChecksumCRC64NVME>7a5hQ478J4A=</ChecksumCRC64NVME></Part>\
         </CompleteMultipartUpload>"
    );
}

/// A prefix over the 5 GiB part ceiling is copied in evenly-split
/// parts, all within the ceiling.
#[test]
fn multipart_rewrite_splits_the_prefix_at_the_part_ceiling() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        copy_part_ok(),
        copy_part_ok(),
        data_part_ok(),
        complete_ok(),
    ]);

    let offset = MAX_COPY_PART_SIZE + 1;
    assert_eq!(rewrite_data_at(&endpoint, offset).unwrap(), offset + 4);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[1].copy_range.as_deref(), Some("bytes=0-2684354560"));
    assert_eq!(
        seen[2].copy_range.as_deref(),
        Some("bytes=2684354561-5368709120")
    );
    assert!(seen[3].path.contains("partNumber=3"), "{}", seen[3].path);
}

/// The expected etag rides the part copies as
/// `x-amz-copy-source-if-match`; unconditional rewrites send no such
/// header.
#[test]
fn expected_etag_is_sent_as_copy_source_if_match() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        copy_part_ok(),
        data_part_ok(),
        complete_ok(),
    ]);

    let offset = 10 * 1024 * 1024;
    rewrite_data_at_if_match(&endpoint, offset, Some("\"expected\"")).unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen[1].copy_if_match.as_deref(), Some("\"expected\""));
    // The data part carries no copy source, hence no precondition.
    assert_eq!(seen[2].copy_if_match, None);
}

/// A 412 on the conditional part copy is the store rejecting the etag
/// precondition: the object changed behind the caller's back. The
/// multipart upload is aborted like any other failure.
#[test]
fn copy_source_if_match_rejection_is_an_etag_mismatch() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        StubResponse::new(412),
        StubResponse::new(204),
    ]);

    let err = rewrite_data_at_if_match(&endpoint, 10 * 1024 * 1024, Some("\"stale\"")).unwrap_err();
    assert_matches!(err, UniversalIoError::AppendEtagMismatch { .. });
    assert_eq!(seen.lock().unwrap().last().unwrap().method, "DELETE");
}

/// A failing step aborts the multipart upload, so it does not linger
/// holding storage for its uploaded parts.
#[test]
fn multipart_rewrite_aborts_on_failure() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        StubResponse::new(500),
        StubResponse::new(204),
    ]);

    let err = rewrite_data_at(&endpoint, 10 * 1024 * 1024).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].method, "DELETE");
    assert_eq!(seen[2].path, "/bucket/dir/append.dat?uploadId=upload-1");
}

/// `CompleteMultipartUpload` can fail after 200 OK, reporting the error
/// in the body; the upload is aborted like any other failure.
#[test]
fn multipart_rewrite_complete_error_in_a_200_body_is_an_error() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        copy_part_ok(),
        data_part_ok(),
        StubResponse::new(200).body("<Error><Code>InternalError</Code></Error>"),
        StubResponse::new(204),
    ]);

    let err = rewrite_data_at(&endpoint, 10 * 1024 * 1024).unwrap_err();
    assert_matches!(err, UniversalIoError::S3 { .. });
    assert!(err.to_string().contains("InternalError"), "{err}");
    assert_eq!(seen.lock().unwrap().last().unwrap().method, "DELETE");
}

#[test]
fn multipart_rewrite_aborts_when_a_copy_has_no_crc64nvme_checksum() {
    let (endpoint, seen) = stub_server(vec![
        initiate_ok(),
        StubResponse::new(200).body(
            "<CopyPartResult><ETag>\"etag-copy\"</ETag>\
             <ChecksumSHA256>unexpected</ChecksumSHA256></CopyPartResult>",
        ),
        StubResponse::new(204),
    ]);
    let err = rewrite_data_at(&endpoint, 10 * 1024 * 1024).unwrap_err();
    assert!(err.to_string().contains("ChecksumCRC64NVME"), "{err}");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].method, "DELETE");
}
