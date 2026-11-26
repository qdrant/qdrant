use bytes::Bytes;
use object_store::ObjectStoreExt as _;
use object_store::aws::AmazonS3;
use object_store::path::Path;

use super::stub::{StubResponse, stub_server};
use crate::BlobBackend as _;
use crate::backends::aws::{AwsConfig, AwsCredentials};

fn assert_crc64nvme_uploads(credentials: AwsCredentials) {
    let (endpoint, seen) = stub_server(vec![
        StubResponse::new(200).header("etag", "\"put\""),
        StubResponse::new(200).body(
            "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>\
             </InitiateMultipartUploadResult>",
        ),
        StubResponse::new(200)
            .header("etag", "\"part\"")
            .header("x-amz-checksum-crc64nvme", "rosUhgp5mIg="),
        StubResponse::new(200).body(
            "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag>\
             </CompleteMultipartUploadResult>",
        ),
    ]);
    let store = AmazonS3::build_store(&AwsConfig {
        bucket: "bucket".into(),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint),
        s3_express: false,
        native_append: false,
        credentials,
    })
    .unwrap();
    io_bridge::BridgeRuntime::global().block_on(async {
        let key = Path::from("checksum.dat");
        store
            .put(&key, Bytes::from_static(b"123456789").into())
            .await
            .unwrap();
        let mut upload = store.put_multipart(&key).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"123456789").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();
    });
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 4);
    for request in [&seen[0], &seen[2]] {
        assert_eq!(request.headers["x-amz-checksum-crc64nvme"], "rosUhgp5mIg=");
        assert!(!request.headers.contains_key("x-amz-checksum-sha256"));
    }
    assert_eq!(seen[1].headers["x-amz-checksum-algorithm"], "CRC64NVME");
    assert!(
        String::from_utf8_lossy(&seen[3].body)
            .contains("<ChecksumCRC64NVME>rosUhgp5mIg=</ChecksumCRC64NVME>")
    );
}

#[test]
fn static_credentials_uploads_use_crc64nvme() {
    assert_crc64nvme_uploads(AwsCredentials::Static {
        access_key_id: "id".into(),
        secret_access_key: "secret".into(),
        session_token: None,
    });
}

#[test]
fn uploads_override_sha256_from_the_environment() {
    const CHILD: &str = "QDRANT_CRC64_CHECKSUM_TEST";
    if std::env::var_os(CHILD).is_some() {
        assert_crc64nvme_uploads(AwsCredentials::Default);
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "append::context::aws_tests::uploads_override_sha256_from_the_environment",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("AWS_CHECKSUM_ALGORITHM", "SHA256")
        .env("AWS_ACCESS_KEY_ID", "id")
        .env("AWS_SECRET_ACCESS_KEY", "secret")
        .env_remove("AWS_SESSION_TOKEN")
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
#[ignore = "requires S3 Express credentials and S3_EXPRESS_INTEGRATION_TEST=1"]
fn crc64nvme_survives_uploads_appends_and_multipart_rewrites() {
    if std::env::var("S3_EXPRESS_INTEGRATION_TEST").as_deref() != Ok("1") {
        return;
    }
    let config = AwsConfig {
        bucket: std::env::var("S3_EXPRESS_BUCKET").expect("S3_EXPRESS_BUCKET"),
        region: Some(std::env::var("AWS_REGION").expect("AWS_REGION")),
        endpoint: None,
        s3_express: true,
        native_append: false,
        credentials: AwsCredentials::Default,
    };
    let store = std::sync::Arc::new(AmazonS3::build_store(&config).unwrap());
    let super::AppendContext::Native(native) = AmazonS3::append_context(&config).unwrap().unwrap()
    else {
        panic!("expected native append");
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = Path::from(format!(
        "_integration_tests/crc64-lifecycle-{}-{nonce}.dat",
        std::process::id()
    ));
    io_bridge::BridgeRuntime::global().block_on(async {
        let seed = Bytes::from(vec![b'x'; 5 * 1024 * 1024]);
        let result = async {
            store.put(&key, seed.clone().into()).await?;
            native
                .append(&store, &key, seed.len() as u64, Bytes::from_static(b"-put"))
                .await?;

            let mut upload = store.put_multipart(&key).await?;
            let uploaded = async {
                upload.put_part(seed.clone().into()).await?;
                upload
                    .put_part(Bytes::from_static(b"-multipart").into())
                    .await?;
                upload.complete().await
            }
            .await;
            if uploaded.is_err() {
                upload.abort().await?;
            }
            uploaded?;
            let len = native
                .append(
                    &store,
                    &key,
                    seed.len() as u64 + 10,
                    Bytes::from_static(b"-native"),
                )
                .await?;
            let etag = store.head(&key).await?.e_tag;
            let len = native
                .part_copy()
                .append(
                    &store,
                    &key,
                    len,
                    Bytes::from_static(b"-rewrite"),
                    etag.as_deref(),
                )
                .await?;
            let len = native
                .append(&store, &key, len, Bytes::from_static(b"-final"))
                .await?;
            let contents = store.get(&key).await?.bytes().await?;
            Ok::<_, Box<dyn std::error::Error>>((len, contents))
        }
        .await;
        store.delete(&key).await.expect("delete test object");
        let (len, contents) = result.unwrap();
        let mut expected = seed.to_vec();
        expected.extend_from_slice(b"-multipart-native-rewrite-final");
        assert_eq!(len, expected.len() as u64);
        assert_eq!(contents.as_ref(), expected);
    });
}
