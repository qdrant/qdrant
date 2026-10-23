use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use io_bridge::{AsyncAppend as _, BridgeRuntime};
use object_store::ObjectStoreExt as _;
use object_store::aws::{AmazonS3, AmazonS3Builder, Checksum};

use crate::ObjectStoreSource;
use crate::backend::BlobBackend as _;
use crate::backends::aws::{AwsConfig, AwsCredentials};

#[test]
#[ignore = "requires S3 Express credentials and S3_EXPRESS_INTEGRATION_TEST=1"]
fn native_append_accepts_crc64nvme_and_rejects_sha256() {
    if std::env::var("S3_EXPRESS_INTEGRATION_TEST").as_deref() != Ok("1") {
        eprintln!(
            "skipping S3 Express integration test; set S3_EXPRESS_INTEGRATION_TEST=1 to enable"
        );
        return;
    }
    let bucket = std::env::var("S3_EXPRESS_BUCKET").expect("S3_EXPRESS_BUCKET");
    let region = std::env::var("AWS_REGION").expect("AWS_REGION");
    let config = AwsConfig {
        bucket: bucket.clone(),
        region: Some(region.clone()),
        endpoint: None,
        s3_express: true,
        native_append: false,
        credentials: AwsCredentials::Default,
    };
    let context = AmazonS3::append_context(&config).unwrap().unwrap();
    let runtime = BridgeRuntime::global();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    for (label, algorithm) in [
        ("sha256", Checksum::SHA256),
        ("crc64nvme", Checksum::CRC64NVME),
    ] {
        let store = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_region(&region)
                .with_s3_express(true)
                .with_checksum_algorithm(algorithm)
                .build()
                .unwrap(),
        );
        let source = ObjectStoreSource::new(store.clone()).with_append_context(context.clone());
        let key = format!(
            "_integration_tests/append-checksum-{}-{nonce}/{label}.dat",
            std::process::id(),
        );
        let object_key = object_store::path::Path::from(key.as_str());

        runtime.block_on(async {
            store
                .put(
                    &object_key,
                    Bytes::from_static(b"checksum-probe-seed").into(),
                )
                .await
                .unwrap();
            let appended = source
                .append(Path::new(&key), 19, Bytes::from_static(b"-append"), None)
                .await;
            let contents = match store.get(&object_key).await {
                Ok(result) => result.bytes().await,
                Err(err) => Err(err),
            };
            store.delete(&object_key).await.expect("delete test object");

            if algorithm == Checksum::CRC64NVME {
                assert_eq!(appended.unwrap(), 26, "{label}");
                assert_eq!(contents.unwrap(), b"checksum-probe-seed-append"[..]);
            } else {
                let err = appended.unwrap_err();
                assert!(err.to_string().contains("Checksum Type mismatch"), "{err}");
                assert_eq!(contents.unwrap(), b"checksum-probe-seed"[..]);
            }
        });
    }
}
