use std::env;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt as _};

use crate::config::{S3Config, S3Credentials};
use crate::runtime::BridgeRuntime;

pub fn rustfs_enabled() -> bool {
    env::var("S3_INTEGRATION_TEST").as_deref() == Ok("1")
}

pub fn rustfs_endpoint() -> String {
    env::var("RUSTFS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

pub fn rustfs_bucket() -> String {
    env::var("RUSTFS_BUCKET").unwrap_or_else(|_| "test-bucket".into())
}

pub fn rustfs_s3_config() -> S3Config {
    S3Config {
        bucket: rustfs_bucket(),
        region: Some("us-east-1".into()),
        endpoint: Some(rustfs_endpoint()),
        credentials: S3Credentials::Static {
            access_key_id: env::var("RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "rustfsadmin".into()),
            secret_access_key: env::var("RUSTFS_SECRET_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            session_token: None,
        },
    }
}

pub fn setup_bucket(runtime: &BridgeRuntime, objects: &[(&str, &[u8])]) -> Arc<dyn ObjectStore> {
    let config = rustfs_s3_config();
    let store = crate::config::build_object_store(&config).expect("build store");
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
