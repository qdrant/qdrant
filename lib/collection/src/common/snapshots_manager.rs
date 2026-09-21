use std::path::{Path, PathBuf};

use common::defaults::APP_USER_AGENT;
use common::tempfile_ext::MaybeTempPath;
use fs_err as fs;
use fs_err::tokio as tokio_fs;
use http::HeaderValue;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ClientOptions, ObjectStoreExt};
use serde::Deserialize;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;

use super::snapshot_stream::{SnapShotStreamLocalFS, SnapshotStream};
use crate::common::file_utils::move_file;
use crate::common::sha_256::hash_file;
use crate::operations::snapshot_ops::{
    SnapshotDescription, get_checksum_path, get_snapshot_description,
};
use crate::operations::snapshot_storage_ops;
use crate::operations::types::{CollectionError, CollectionResult};

#[derive(Clone, Deserialize, Debug, Default)]
pub struct SnapshotsConfig {
    pub snapshots_storage: SnapshotsStorageConfig,
    pub s3_config: Option<S3Config>,
    pub gcs_config: Option<GcsConfig>,
    pub azure_config: Option<AzureConfig>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotsStorageConfig {
    #[default]
    Local,
    S3,
    #[serde(alias = "gcp")]
    Gcs,
    Azure,
}

/// Amazon S3 (or S3-compatible) connection parameters.
///
/// Every field except `bucket` is optional. Missing values are resolved from the
/// `AWS_*` environment, so credentials may come from the default credential chain.
#[derive(Clone, Deserialize, Debug, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint_url: Option<String>,
}

/// Google Cloud Storage connection parameters.
///
/// Every field except `bucket` is optional. Without explicit credentials the client
/// resolves Application Default Credentials: `GOOGLE_*` environment variables, the
/// gcloud credentials file, or the metadata server on GCE and GKE.
#[derive(Clone, Deserialize, Debug, Default)]
pub struct GcsConfig {
    pub bucket: String,
    /// Path to a service account JSON key file.
    pub service_account_path: Option<String>,
    /// Inline contents of a service account JSON key.
    pub service_account_key: Option<String>,
    /// Path to an `application_default_credentials.json` file.
    pub application_credentials_path: Option<String>,
    /// Custom base URL, for emulators such as fake-gcs-server.
    pub endpoint_url: Option<String>,
}

/// Azure Blob Storage connection parameters.
///
/// Every field except `account` and `container` is optional. Without explicit
/// credentials the client resolves the default chain: `AZURE_*` environment
/// variables, managed identity, or the Azure CLI.
#[derive(Clone, Deserialize, Debug, Default)]
pub struct AzureConfig {
    /// Storage account name, without the `.blob.core.windows.net` suffix.
    pub account: String,
    pub container: String,
    /// Shared access key of the storage account.
    pub access_key: Option<String>,
    /// Shared access signature, as the query string issued by Azure.
    pub sas_token: Option<String>,
    /// Service principal client secret flow, all three fields go together.
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub tenant_id: Option<String>,
    /// Custom endpoint URL, for emulators such as Azurite.
    pub endpoint_url: Option<String>,
}

pub struct SnapshotStorageCloud {
    client: Box<dyn object_store::ObjectStore>,
}

pub struct SnapshotStorageLocalFS;

pub enum SnapshotStorageManager {
    LocalFS(SnapshotStorageLocalFS),
    /// Any `object_store` backend, all of them share the same operations.
    Cloud(SnapshotStorageCloud),
}

/// HTTP client options shared by all object store backends.
///
/// Identifies Qdrant with its user agent so providers can attribute snapshot traffic.
/// Plain HTTP is only allowed for explicitly configured `http://` endpoints, which
/// are typically local emulators.
fn client_options(endpoint_url: Option<&str>) -> ClientOptions {
    let allow_http = endpoint_url.is_some_and(|url| url.starts_with("http://"));
    let mut options = ClientOptions::new().with_allow_http(allow_http);
    if let Ok(user_agent) = HeaderValue::from_str(APP_USER_AGENT.as_str()) {
        options = options.with_user_agent(user_agent);
    }
    options
}

fn build_s3_client(config: Option<&S3Config>) -> CollectionResult<object_store::aws::AmazonS3> {
    let endpoint_url = config.and_then(|config| config.endpoint_url.as_deref());
    let mut builder = AmazonS3Builder::from_env().with_client_options(client_options(endpoint_url));

    if let Some(config) = config {
        builder = builder.with_bucket_name(&config.bucket);
        if let Some(access_key) = &config.access_key {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &config.secret_key {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint_url) = &config.endpoint_url {
            builder = builder.with_endpoint(endpoint_url);
        }
    }

    builder
        .build()
        .map_err(|e| CollectionError::service_error(format!("Failed to create S3 client: {e}")))
}

fn build_gcs_client(
    config: Option<&GcsConfig>,
) -> CollectionResult<object_store::gcp::GoogleCloudStorage> {
    let endpoint_url = config.and_then(|config| config.endpoint_url.as_deref());
    let mut builder =
        GoogleCloudStorageBuilder::from_env().with_client_options(client_options(endpoint_url));

    if let Some(config) = config {
        builder = builder.with_bucket_name(&config.bucket);
        if let Some(path) = &config.service_account_path {
            builder = builder.with_service_account_path(path);
        }
        if let Some(key) = &config.service_account_key {
            builder = builder.with_service_account_key(key);
        }
        if let Some(path) = &config.application_credentials_path {
            builder = builder.with_application_credentials(path);
        }
        if let Some(endpoint_url) = &config.endpoint_url {
            builder = builder.with_base_url(endpoint_url);
        }
    }

    builder
        .build()
        .map_err(|e| CollectionError::service_error(format!("Failed to create GCS client: {e}")))
}

fn build_azure_client(
    config: Option<&AzureConfig>,
) -> CollectionResult<object_store::azure::MicrosoftAzure> {
    let endpoint_url = config.and_then(|config| config.endpoint_url.as_deref());
    let mut builder =
        MicrosoftAzureBuilder::from_env().with_client_options(client_options(endpoint_url));

    if let Some(config) = config {
        builder = builder
            .with_account(&config.account)
            .with_container_name(&config.container);
        if let Some(access_key) = &config.access_key {
            builder = builder.with_access_key(access_key);
        }
        if let Some(sas_token) = &config.sas_token {
            builder = builder.with_config(AzureConfigKey::SasKey, sas_token);
        }
        if let Some(client_id) = &config.client_id {
            builder = builder.with_client_id(client_id);
        }
        if let Some(client_secret) = &config.client_secret {
            builder = builder.with_client_secret(client_secret);
        }
        if let Some(tenant_id) = &config.tenant_id {
            builder = builder.with_tenant_id(tenant_id);
        }
        if let Some(endpoint_url) = &config.endpoint_url {
            builder = builder.with_endpoint(endpoint_url.clone());
        }
    }

    builder
        .build()
        .map_err(|e| CollectionError::service_error(format!("Failed to create Azure client: {e}")))
}

/// Assert a snapshot name is a plain file name, not a path.
///
/// Backends join it onto a snapshots directory, and `Path::join` drops the base if it is absolute.
fn validate_snapshot_name(snapshot_name: &str) -> CollectionResult<()> {
    if Path::new(snapshot_name).file_name() != Some(snapshot_name.as_ref()) {
        return Err(CollectionError::not_found(format!(
            "Snapshot {snapshot_name}"
        )));
    }

    Ok(())
}

impl SnapshotStorageManager {
    /// Create a snapshot storage manager from the configured backend.
    pub fn new(snapshots_config: &SnapshotsConfig) -> CollectionResult<Self> {
        let client: Box<dyn object_store::ObjectStore> = match snapshots_config.snapshots_storage {
            SnapshotsStorageConfig::Local => {
                return Ok(SnapshotStorageManager::LocalFS(SnapshotStorageLocalFS));
            }
            SnapshotsStorageConfig::S3 => {
                Box::new(build_s3_client(snapshots_config.s3_config.as_ref())?)
            }
            SnapshotsStorageConfig::Gcs => {
                Box::new(build_gcs_client(snapshots_config.gcs_config.as_ref())?)
            }
            SnapshotsStorageConfig::Azure => {
                Box::new(build_azure_client(snapshots_config.azure_config.as_ref())?)
            }
        };
        Ok(SnapshotStorageManager::Cloud(SnapshotStorageCloud {
            client,
        }))
    }

    pub async fn delete_snapshot(&self, snapshot_name: &Path) -> CollectionResult<bool> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.delete_snapshot(snapshot_name).await
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl.delete_snapshot(snapshot_name).await
            }
        }
    }

    pub async fn list_snapshots(
        &self,
        directory: &Path,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.list_snapshots(directory).await
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl.list_snapshots(directory).await
            }
        }
    }

    /// Store file in the snapshot storage.
    /// On success, the `source_path` is deleted.
    pub async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        debug_assert_ne!(
            source_path, target_path,
            "Source and target paths must be different"
        );
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.store_file(source_path, target_path).await
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl.store_file(source_path, target_path).await
            }
        }
    }

    pub async fn get_stored_file(
        &self,
        storage_path: &Path,
        local_path: &Path,
    ) -> CollectionResult<()> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.get_stored_file(storage_path, local_path).await
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl.get_stored_file(storage_path, local_path).await
            }
        }
    }

    /// Get the storage path for a collection snapshot, confined to `snapshots_path`
    pub fn get_snapshot_path(
        &self,
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        validate_snapshot_name(snapshot_name)?;

        match self {
            SnapshotStorageManager::LocalFS(_storage_impl) => {
                SnapshotStorageLocalFS::get_snapshot_path(snapshots_path, snapshot_name)
            }
            SnapshotStorageManager::Cloud(_storage_impl) => Ok(
                SnapshotStorageCloud::get_snapshot_path(snapshots_path, snapshot_name),
            ),
        }
    }

    /// Get the storage path for a full snapshot, confined to `snapshots_path`
    pub fn get_full_snapshot_path(
        &self,
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        validate_snapshot_name(snapshot_name)?;

        match self {
            SnapshotStorageManager::LocalFS(_storage_impl) => {
                SnapshotStorageLocalFS::get_full_snapshot_path(snapshots_path, snapshot_name)
            }
            SnapshotStorageManager::Cloud(_storage_impl) => Ok(
                SnapshotStorageCloud::get_full_snapshot_path(snapshots_path, snapshot_name),
            ),
        }
    }

    pub async fn get_snapshot_file(
        &self,
        snapshot_path: &Path,
        temp_dir: &Path,
    ) -> CollectionResult<MaybeTempPath> {
        match self {
            SnapshotStorageManager::LocalFS(_storage_impl) => {
                SnapshotStorageLocalFS::get_snapshot_file(snapshot_path, temp_dir)
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl
                    .get_snapshot_file(snapshot_path, temp_dir)
                    .await
            }
        }
    }

    pub async fn get_snapshot_stream(
        &self,
        snapshot_path: &Path,
    ) -> CollectionResult<SnapshotStream> {
        match self {
            SnapshotStorageManager::LocalFS(_storage_impl) => {
                Ok(SnapshotStorageLocalFS::get_snapshot_stream(snapshot_path))
            }
            SnapshotStorageManager::Cloud(storage_impl) => {
                storage_impl.get_snapshot_stream(snapshot_path).await
            }
        }
    }
}

impl SnapshotStorageLocalFS {
    async fn delete_snapshot(&self, snapshot_path: &Path) -> CollectionResult<bool> {
        let checksum_path = get_checksum_path(snapshot_path);
        let (delete_snapshot, delete_checksum) = tokio::join!(
            tokio_fs::remove_file(snapshot_path),
            tokio_fs::remove_file(checksum_path),
        );

        #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
        delete_snapshot.map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                CollectionError::not_found(format!("Snapshot {snapshot_path:?}"))
            }
            _ => e.into(),
        })?;

        // We might not have a checksum file for the snapshot, ignore deletion errors in that case
        if let Err(err) = delete_checksum {
            log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
        }

        Ok(true)
    }

    async fn list_snapshots(&self, directory: &Path) -> CollectionResult<Vec<SnapshotDescription>> {
        let mut entries = match tokio_fs::read_dir(directory).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut snapshots = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if !path.is_dir() && path.extension().is_some_and(|ext| ext == "snapshot") {
                snapshots.push(get_snapshot_description(&path).await?);
            }
        }

        Ok(snapshots)
    }

    /// Stores a snapshot in local storage and writes its checksum first.
    ///
    /// The checksum file is explicitly flushed and closed before moving the
    /// snapshot into place, so checksum readers do not observe a partially
    /// written checksum file.
    async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        // Steps:
        //
        // 1. Make sure that the target directory exists.
        // 2. Compute the checksum of the source file.
        // 3. Generate temporary file name, which should be used on the same file system as the target directory.
        // 4. Move or copy the source file to the temporary file. (move might not be possible if the source and target are on different file systems)
        // 5. Move the temporary file to the target file. (move is atomic, copy is not)

        if let Some(target_dir) = target_path.parent() {
            fs::create_dir_all(target_dir)?;
        }

        // Move snapshot to permanent location.
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, because copy is not atomic.
        // So we copy to the final location with a temporary name and then rename atomically.
        let target_path_tmp = TempPath::try_from_path(target_path.with_extension("tmp"))?;

        // Write checksum to a temporary path first. We only promote it to the
        // final checksum path after the snapshot itself is persisted.
        let checksum_path = get_checksum_path(target_path);
        let checksum_path_tmp = TempPath::try_from_path(checksum_path.with_extension("tmp"))?;
        let checksum = hash_file(source_path).await?;
        let mut file = tokio_fs::File::create(&checksum_path_tmp).await?;
        file.write_all(checksum.as_bytes()).await?;
        file.flush().await?;
        drop(file);

        move_file(&source_path, &target_path_tmp).await?;
        target_path_tmp.persist(target_path).map_err(|e| e.error)?;

        if let Err(err) = checksum_path_tmp.persist(&checksum_path) {
            // Snapshot persisted but checksum promotion failed: rollback to
            // avoid leaving partial state.
            let _ = tokio_fs::remove_file(target_path).await;
            return Err(err.error.into());
        }
        get_snapshot_description(target_path).await
    }

    async fn get_stored_file(
        &self,
        storage_path: &Path,
        local_path: &Path,
    ) -> CollectionResult<()> {
        if let Some(target_dir) = local_path.parent()
            && !target_dir.exists()
        {
            fs::create_dir_all(target_dir)?;
        }

        if storage_path != local_path {
            move_file(&storage_path, &local_path).await?;
        }
        Ok(())
    }

    /// Get absolute file path for a full snapshot by name
    ///
    /// This enforces the file to be inside the snapshots directory
    fn get_full_snapshot_path(
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = fs::canonicalize(snapshots_path).map_err(|_| {
            CollectionError::not_found(format!("Snapshot directory: {}", snapshots_path.display()))
        })?;

        let absolute_snapshot_path = fs::canonicalize(absolute_snapshot_dir.join(snapshot_name))
            .map_err(|_| CollectionError::not_found(format!("Snapshot {snapshot_name}")))?;

        if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        if !absolute_snapshot_path.is_file() {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        Ok(absolute_snapshot_path)
    }

    /// Get absolute file path for a collection snapshot by name
    ///
    /// This enforces the file to be inside the snapshots directory
    fn get_snapshot_path(snapshots_path: &Path, snapshot_name: &str) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = fs::canonicalize(snapshots_path).map_err(|_| {
            CollectionError::not_found(format!("Snapshot directory: {}", snapshots_path.display()))
        })?;

        let absolute_snapshot_path = fs::canonicalize(absolute_snapshot_dir.join(snapshot_name))
            .map_err(|_| CollectionError::not_found(format!("Snapshot {snapshot_name}")))?;

        if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        if !absolute_snapshot_path.is_file() {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        Ok(absolute_snapshot_path)
    }

    fn get_snapshot_file(
        snapshot_path: &Path,
        _temp_dir: &Path,
    ) -> CollectionResult<MaybeTempPath> {
        if !snapshot_path.exists() {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_path:?}"
            )));
        }
        Ok(MaybeTempPath::Persistent(snapshot_path.to_path_buf()))
    }

    fn get_snapshot_stream(snapshot_path: &Path) -> SnapshotStream {
        SnapshotStream::LocalFS(SnapShotStreamLocalFS {
            snapshot_path: snapshot_path.to_path_buf(),
        })
    }
}

impl SnapshotStorageCloud {
    async fn delete_snapshot(&self, snapshot_path: &Path) -> CollectionResult<bool> {
        snapshot_storage_ops::delete_snapshot(&self.client, snapshot_path).await
    }

    async fn list_snapshots(&self, directory: &Path) -> CollectionResult<Vec<SnapshotDescription>> {
        snapshot_storage_ops::list_snapshot_descriptions(&self.client, directory).await
    }

    async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        snapshot_storage_ops::multipart_upload(&self.client, source_path, target_path).await?;
        tokio_fs::remove_file(source_path).await?;
        snapshot_storage_ops::get_snapshot_description(&self.client, target_path).await
    }

    async fn get_stored_file(
        &self,
        storage_path: &Path,
        local_path: &Path,
    ) -> CollectionResult<()> {
        if let Some(target_dir) = local_path.parent()
            && !target_dir.exists()
        {
            fs::create_dir_all(target_dir)?;
        }
        if storage_path != local_path {
            // download snapshot from cloud storage to local path
            snapshot_storage_ops::download_snapshot(&self.client, storage_path, local_path).await?;
        }
        Ok(())
    }

    /// `snapshot_name` must pass [`validate_snapshot_name`] first: unlike the local filesystem,
    /// object storage has no `canonicalize` to confine the key afterwards.
    fn get_snapshot_path(snapshots_path: &Path, snapshot_name: &str) -> PathBuf {
        let absolute_snapshot_dir = snapshots_path;
        absolute_snapshot_dir.join(snapshot_name)
    }

    /// `snapshot_name` must pass [`validate_snapshot_name`] first.
    fn get_full_snapshot_path(snapshots_path: &Path, snapshot_name: &str) -> PathBuf {
        let absolute_snapshot_dir = snapshots_path;
        absolute_snapshot_dir.join(snapshot_name)
    }

    async fn get_snapshot_file(
        &self,
        snapshot_path: &Path,
        temp_dir: &Path,
    ) -> CollectionResult<MaybeTempPath> {
        let temp_path = tempfile::Builder::new()
            .prefix(
                snapshot_path
                    .file_stem()
                    .ok_or_else(|| CollectionError::bad_request("Invalid snapshot path"))?,
            )
            .suffix(".snapshot")
            .tempfile_in(temp_dir)?
            .into_temp_path();

        snapshot_storage_ops::download_snapshot(&self.client, snapshot_path, &temp_path).await?;

        Ok(MaybeTempPath::Temporary(temp_path))
    }

    pub async fn get_snapshot_stream(
        &self,
        snapshot_path: &Path,
    ) -> CollectionResult<SnapshotStream> {
        let snapshot_path = snapshot_storage_ops::trim_dot_slash(snapshot_path)?;
        #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
        let download = self.client.get(&snapshot_path).await.map_err(|e| match e {
            object_store::Error::NotFound { path, source } => {
                CollectionError::not_found(format!("Snapshot {path} does not exist: {source}"))
            }
            _ => CollectionError::service_error(format!("Failed to get {snapshot_path}: {e}")),
        })?;
        Ok(SnapshotStream::new_stream(download.into_stream(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Snapshot names that escape the snapshots directory.
    const TRAVERSING_NAMES: &[&str] = &[
        "/snapshots/other-collection/victim.snapshot",
        "/etc/passwd",
        "other-collection/victim.snapshot",
        "../other-collection/victim.snapshot",
        "./victim.snapshot",
        "..",
        ".",
        "",
    ];

    fn s3_manager() -> SnapshotStorageManager {
        SnapshotStorageManager::new(&SnapshotsConfig {
            snapshots_storage: SnapshotsStorageConfig::S3,
            s3_config: Some(S3Config {
                bucket: "test-bucket".into(),
                region: Some("us-east-1".into()),
                access_key: Some("test-access-key".into()),
                secret_key: Some("test-secret-key".into()),
                endpoint_url: Some("http://localhost:9000".into()),
            }),
            ..Default::default()
        })
        .unwrap()
    }

    /// Azurite's well-known development account key.
    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn azure_manager() -> SnapshotStorageManager {
        SnapshotStorageManager::new(&SnapshotsConfig {
            snapshots_storage: SnapshotsStorageConfig::Azure,
            azure_config: Some(AzureConfig {
                account: "devstoreaccount1".into(),
                container: "test-container".into(),
                access_key: Some(AZURITE_ACCOUNT_KEY.into()),
                endpoint_url: Some("http://localhost:10000/devstoreaccount1".into()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap()
    }

    fn gcs_manager() -> SnapshotStorageManager {
        let key_file = tempfile::NamedTempFile::new().unwrap();
        // Only the shape matters, the key is never used to sign a request.
        fs::write(
            key_file.path(),
            serde_json::json!({
                "gcs_base_url": "http://localhost:4443",
                "disable_oauth": true,
                "client_email": "",
                "private_key_id": "",
                "private_key": "",
            })
            .to_string(),
        )
        .unwrap();

        SnapshotStorageManager::new(&SnapshotsConfig {
            snapshots_storage: SnapshotsStorageConfig::Gcs,
            gcs_config: Some(GcsConfig {
                bucket: "test-bucket".into(),
                service_account_path: Some(key_file.path().to_str().unwrap().into()),
                endpoint_url: Some("http://localhost:4443".into()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap()
    }

    /// The legacy shape keeps parsing, and the new backends parse with their own blocks.
    #[test]
    fn snapshots_config_deserializes_all_backends() {
        let legacy: SnapshotsConfig = serde_json::from_value(serde_json::json!({
            "snapshots_storage": "s3",
            "s3_config": { "bucket": "b", "region": "us-east-1" },
        }))
        .unwrap();
        assert_eq!(legacy.snapshots_storage, SnapshotsStorageConfig::S3);
        assert_eq!(legacy.s3_config.unwrap().bucket, "b");
        assert!(legacy.gcs_config.is_none());
        assert!(legacy.azure_config.is_none());

        let local: SnapshotsConfig =
            serde_json::from_value(serde_json::json!({ "snapshots_storage": "local" })).unwrap();
        assert_eq!(local.snapshots_storage, SnapshotsStorageConfig::Local);

        for storage in ["gcs", "gcp"] {
            let gcs: SnapshotsConfig = serde_json::from_value(serde_json::json!({
                "snapshots_storage": storage,
                "gcs_config": { "bucket": "b", "service_account_path": "/key.json" },
            }))
            .unwrap();
            assert_eq!(gcs.snapshots_storage, SnapshotsStorageConfig::Gcs);
            let gcs_config = gcs.gcs_config.unwrap();
            assert_eq!(gcs_config.bucket, "b");
            assert_eq!(
                gcs_config.service_account_path.as_deref(),
                Some("/key.json")
            );
        }

        let azure: SnapshotsConfig = serde_json::from_value(serde_json::json!({
            "snapshots_storage": "azure",
            "azure_config": { "account": "a", "container": "c", "sas_token": "sv=1&sig=x" },
        }))
        .unwrap();
        assert_eq!(azure.snapshots_storage, SnapshotsStorageConfig::Azure);
        let azure_config = azure.azure_config.unwrap();
        assert_eq!(azure_config.account, "a");
        assert_eq!(azure_config.container, "c");
        assert_eq!(azure_config.sas_token.as_deref(), Some("sv=1&sig=x"));
    }

    /// Cloud backends without their config block still build, reading the environment.
    #[test]
    fn cloud_backends_build_without_config_block() {
        for storage in [SnapshotsStorageConfig::Gcs, SnapshotsStorageConfig::Azure] {
            let manager = SnapshotStorageManager::new(&SnapshotsConfig {
                snapshots_storage: storage,
                ..Default::default()
            });
            // Without a bucket or account the builder must fail, not panic.
            assert!(
                matches!(manager, Err(CollectionError::ServiceError { .. })),
                "{storage:?}"
            );
        }
    }

    #[test]
    fn validate_snapshot_name_accepts_plain_file_names() {
        for name in ["my.snapshot", "collection-2024-01-01-00-00-00.snapshot"] {
            validate_snapshot_name(name).unwrap();
        }
    }

    #[test]
    fn validate_snapshot_name_rejects_paths() {
        for name in TRAVERSING_NAMES {
            assert!(
                matches!(
                    validate_snapshot_name(name),
                    Err(CollectionError::NotFound { .. }),
                ),
                "snapshot name {name:?} escapes the snapshots directory and must be rejected",
            );
        }
    }

    /// GHSA-p9c2-9vq2-gxh8: an unchecked name let a caller scoped to one collection read and
    /// delete any object in the bucket.
    #[test]
    fn object_storage_rejects_traversing_snapshot_name() {
        for manager in [s3_manager(), gcs_manager(), azure_manager()] {
            object_storage_rejects_traversing_snapshot_name_for(&manager);
        }
    }

    fn object_storage_rejects_traversing_snapshot_name_for(manager: &SnapshotStorageManager) {
        let snapshots_path = Path::new("snapshots/my-collection");

        for name in TRAVERSING_NAMES {
            assert!(
                matches!(
                    manager.get_snapshot_path(snapshots_path, name),
                    Err(CollectionError::NotFound { .. }),
                ),
                "snapshot name {name:?} escapes the collection prefix and must be rejected",
            );
            assert!(
                matches!(
                    manager.get_full_snapshot_path(Path::new("snapshots"), name),
                    Err(CollectionError::NotFound { .. }),
                ),
                "snapshot name {name:?} escapes the snapshots prefix and must be rejected",
            );
        }
    }

    #[test]
    fn object_storage_keeps_plain_snapshot_name_under_prefix() {
        let manager = s3_manager();
        let snapshots_path = Path::new("snapshots/my-collection");

        assert_eq!(
            manager
                .get_snapshot_path(snapshots_path, "my.snapshot")
                .unwrap(),
            Path::new("snapshots/my-collection/my.snapshot"),
        );
        assert_eq!(
            manager
                .get_full_snapshot_path(Path::new("snapshots"), "full.snapshot")
                .unwrap(),
            Path::new("snapshots/full.snapshot"),
        );
    }

    #[test]
    fn local_storage_rejects_traversing_snapshot_name() {
        let manager = SnapshotStorageManager::new(&SnapshotsConfig {
            snapshots_storage: SnapshotsStorageConfig::Local,
            ..Default::default()
        })
        .unwrap();

        let snapshots_dir = tempfile::Builder::new().tempdir().unwrap();
        let snapshots_path = snapshots_dir.path();

        // Traversal target
        let other_collection = snapshots_path.join("other-collection");
        fs::create_dir_all(&other_collection).unwrap();
        fs::write(other_collection.join("victim.snapshot"), b"victim").unwrap();

        // The caller's own collection
        let my_collection = snapshots_path.join("my-collection");
        fs::create_dir_all(&my_collection).unwrap();
        fs::write(my_collection.join("my.snapshot"), b"mine").unwrap();

        for name in TRAVERSING_NAMES {
            assert!(
                matches!(
                    manager.get_snapshot_path(&my_collection, name),
                    Err(CollectionError::NotFound { .. }),
                ),
                "snapshot name {name:?} escapes the collection directory and must be rejected",
            );
        }

        // Reaching into a collection directory is not a full snapshot either
        assert!(matches!(
            manager.get_full_snapshot_path(snapshots_path, "other-collection/victim.snapshot"),
            Err(CollectionError::NotFound { .. }),
        ));

        // An ordinary name still resolves
        assert_eq!(
            manager
                .get_snapshot_path(&my_collection, "my.snapshot")
                .unwrap(),
            fs::canonicalize(my_collection.join("my.snapshot")).unwrap(),
        );
    }
}
