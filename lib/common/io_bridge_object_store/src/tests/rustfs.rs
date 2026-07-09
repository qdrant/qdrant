use std::env;
use std::sync::Arc;

use bytes::Bytes;
use io_bridge::BridgeRuntime;
use object_store::ObjectStoreExt as _;
use object_store::aws::AmazonS3;

use crate::backend::BlobBackend;
use crate::backends::aws::{AwsConfig, AwsCredentials};

pub fn rustfs_enabled() -> bool {
    env::var("S3_INTEGRATION_TEST").as_deref() == Ok("1")
}

pub fn rustfs_endpoint() -> String {
    env::var("RUSTFS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

pub fn rustfs_bucket() -> String {
    env::var("RUSTFS_BUCKET").unwrap_or_else(|_| "test-bucket".into())
}

pub fn rustfs_aws_config() -> AwsConfig {
    AwsConfig {
        bucket: rustfs_bucket(),
        region: Some("us-east-1".into()),
        endpoint: Some(rustfs_endpoint()),
        s3_express: false,
        credentials: AwsCredentials::Static {
            access_key_id: env::var("RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "rustfsadmin".into()),
            secret_access_key: env::var("RUSTFS_SECRET_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            session_token: None,
        },
    }
}

pub fn setup_bucket(runtime: &BridgeRuntime, objects: &[(&str, &[u8])]) -> Arc<AmazonS3> {
    let config = rustfs_aws_config();
    let store = Arc::new(AmazonS3::build_store(&config).expect("build store"));
    runtime.block_on(async {
        for (key, bytes) in objects {
            store
                .put(
                    &object_store::path::Path::from(*key),
                    Bytes::copy_from_slice(bytes).into(),
                )
                .await
                .expect("rustfs put");
        }
    });
    store
}
