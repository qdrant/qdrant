use std::path::Path;

use actix_multipart::form::tempfile::TempFile;
use actix_multipart::form::MultipartForm;
use actix_web::{delete, get, post, put, web, Responder, Result};
use actix_web_validator as valid;
use collection::common::file_utils::move_file;
use collection::common::sha_256::{hash_file, hashes_equal};
use collection::common::snapshot_stream::SnapshotStream;
use collection::operations::snapshot_ops::{
    ShardSnapshotRecover, SnapshotPriority, SnapshotRecover,
};
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::shard::ShardId;
use futures::{FutureExt as _, TryFutureExt as _};
use reqwest::Url;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::content_manager::snapshots::recover::do_recover_from_snapshot;
use storage::content_manager::snapshots::{
    do_create_full_snapshot, do_delete_collection_snapshot, do_delete_full_snapshot,
    do_list_full_snapshots,
};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use uuid::Uuid;
use validator::Validate;

use super::{CollectionPath, StrictCollectionPath};
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, HttpError};
use crate::common;
use crate::common::collections::*;
use crate::common::http_client::HttpClient;

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct SnapshotUploadingParam {
    pub wait: Option<bool>,
    pub priority: Option<SnapshotPriority>,

    /// Optional SHA256 checksum to verify snapshot integrity before recovery.
    #[serde(default)]
    #[validate(custom(function = "::common::validation::validate_sha256_hash"))]
    pub checksum: Option<String>,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct SnapshottingParam {
    pub wait: Option<bool>,
}

#[derive(MultipartForm)]
pub struct SnapshottingForm {
    snapshot: TempFile,
}

// Actix specific code
pub async fn do_get_full_snapshot(
    toc: &TableOfContent,
    access: Access,
    snapshot_name: &str,
) -> Result<SnapshotStream, HttpError> {
    access.check_global_access(AccessRequirements::new())?;
    let snapshots_storage_manager = toc.get_snapshots_storage_manager()?;
    let snapshot_path =
        snapshots_storage_manager.get_full_snapshot_path(toc.snapshots_path(), snapshot_name)?;
    let snapshot_stream = snapshots_storage_manager
        .get_snapshot_stream(&snapshot_path)
        .await?;
    Ok(snapshot_stream)
}

pub async fn do_save_uploaded_snapshot(
    toc: &TableOfContent,
    collection_name: &str,
    snapshot: TempFile,
) -> Result<Url, StorageError> {
    let filename = snapshot
        .file_name
        // Sanitize the file name:
        // - only take the top level path (no directories such as ../)
        // - require the file name to be valid UTF-8
        .and_then(|x| {
            Path::new(&x)
                .file_name()
                .map(|filename| filename.to_owned())
        })
        .and_then(|x| x.to_str().map(|x| x.to_owned()))
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let collection_snapshot_path = toc.snapshots_path_for_collection(collection_name);
    if !collection_snapshot_path.exists() {
        log::debug!(
            "Creating missing collection snapshots directory for {}",
            collection_name
        );
        toc.create_snapshots_path(collection_name).await?;
    }

    let path = collection_snapshot_path.join(filename);

    move_file(snapshot.file.path(), &path).await?;

    let absolute_path = path.canonicalize()?;

    let snapshot_location = Url::from_file_path(&absolute_path).map_err(|_| {
        StorageError::service_error(format!(
            "Failed to convert path to URL: {}",
            absolute_path.display()
        ))
    })?;

    Ok(snapshot_location)
}

// Actix specific code
pub async fn do_get_snapshot(
    toc: &TableOfContent,
    access: Access,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<SnapshotStream, HttpError> {
    let collection_pass = access
        .check_collection_access(collection_name, AccessRequirements::new().whole().extras())?;
    let collection: tokio::sync::RwLockReadGuard<collection::collection::Collection> =
        toc.get_collection(&collection_pass).await?;
    let snapshot_storage_manager = collection.get_snapshots_storage_manager()?;
    let snapshot_path =
        snapshot_storage_manager.get_snapshot_path(collection.snapshots_path(), snapshot_name)?;
    let snapshot_stream = snapshot_storage_manager
        .get_snapshot_stream(&snapshot_path)
        .await?;
    Ok(snapshot_stream)
}

#[get("/collections/{name}/snapshots")]
async fn list_snapshots(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // Nothing to verify.
    let pass = new_unchecked_verification_pass();

    helpers::time(do_list_snapshots(
        dispatcher.toc(&access, &pass),
        access,
        &path,
    ))
    .await
}

#[post("/collections/{name}/snapshots")]
async fn create_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    params: valid::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // Nothing to verify.
    let pass = new_unchecked_verification_pass();

    let collection_name = path.into_inner();

    let future = async move {
        do_create_snapshot(
            dispatcher.toc(&access, &pass).clone(),
            access,
            &collection_name,
        )
        .await
    };

    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

#[post("/collections/{name}/snapshots/upload")]
async fn upload_snapshot(
    dispatcher: web::Data<Dispatcher>,
    http_client: web::Data<HttpClient>,
    collection: valid::Path<StrictCollectionPath>,
    MultipartForm(form): MultipartForm<SnapshottingForm>,
    params: valid::Query<SnapshotUploadingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let wait = params.wait;

    // Nothing to verify.
    let pass = new_unchecked_verification_pass();

    let future = async move {
        let snapshot = form.snapshot;

        access.check_global_access(AccessRequirements::new().manage())?;

        if let Some(checksum) = &params.checksum {
            let snapshot_checksum = hash_file(snapshot.file.path()).await?;
            if !hashes_equal(snapshot_checksum.as_str(), checksum.as_str()) {
                return Err(StorageError::checksum_mismatch(snapshot_checksum, checksum));
            }
        }

        let snapshot_location =
            do_save_uploaded_snapshot(dispatcher.toc(&access, &pass), &collection.name, snapshot)
                .await?;

        // Snapshot is a local file, we do not need an API key for that
        let http_client = http_client.client(None)?;

        let snapshot_recover = SnapshotRecover {
            location: snapshot_location,
            priority: params.priority,
            checksum: None,
            api_key: None,
        };

        do_recover_from_snapshot(
            dispatcher.get_ref(),
            &collection.name,
            snapshot_recover,
            access,
            http_client,
        )
        .await
    };

    helpers::time_or_accept(future, wait.unwrap_or(true)).await
}

#[put("/collections/{name}/snapshots/recover")]
async fn recover_from_snapshot(
    dispatcher: web::Data<Dispatcher>,
    http_client: web::Data<HttpClient>,
    collection: valid::Path<CollectionPath>,
    request: valid::Json<SnapshotRecover>,
    params: valid::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let future = async move {
        let snapshot_recover = request.into_inner();
        let http_client = http_client.client(snapshot_recover.api_key.as_deref())?;

        do_recover_from_snapshot(
            dispatcher.get_ref(),
            &collection.name,
            snapshot_recover,
            access,
            http_client,
        )
        .await
    };

    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

#[get("/collections/{name}/snapshots/{snapshot_name}")]
async fn get_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, String)>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // Nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection_name, snapshot_name) = path.into_inner();
    do_get_snapshot(
        dispatcher.toc(&access, &pass),
        access,
        &collection_name,
        &snapshot_name,
    )
    .await
}

#[get("/snapshots")]
async fn list_full_snapshots(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    helpers::time(do_list_full_snapshots(
        dispatcher.toc(&access, &pass),
        access,
    ))
    .await
}

#[post("/snapshots")]
async fn create_full_snapshot(
    dispatcher: web::Data<Dispatcher>,
    params: valid::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let future = async move { do_create_full_snapshot(dispatcher.get_ref(), access).await };
    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

#[get("/snapshots/{snapshot_name}")]
async fn get_full_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let snapshot_name = path.into_inner();
    do_get_full_snapshot(dispatcher.toc(&access, &pass), access, &snapshot_name).await
}

#[delete("/snapshots/{snapshot_name}")]
async fn delete_full_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    params: valid::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let future = async move {
        let snapshot_name = path.into_inner();
        do_delete_full_snapshot(dispatcher.get_ref(), access, &snapshot_name).await
    };

    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

#[delete("/collections/{name}/snapshots/{snapshot_name}")]
async fn delete_collection_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, String)>,
    params: valid::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let future = async move {
        let (collection_name, snapshot_name) = path.into_inner();

        do_delete_collection_snapshot(
            dispatcher.get_ref(),
            access,
            &collection_name,
            &snapshot_name,
        )
        .await
    };

    helpers::time_or_accept(future, params.wait.unwrap_or(true)).await
}

#[get("/collections/{collection}/shards/{shard}/snapshots")]
async fn list_shard_snapshots(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId)>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard) = path.into_inner();

    let future = common::snapshots::list_shard_snapshots(
        dispatcher.toc(&access, &pass).clone(),
        access,
        collection,
        shard,
    )
    .map_err(Into::into);

    helpers::time(future).await
}

#[post("/collections/{collection}/shards/{shard}/snapshots")]
async fn create_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard) = path.into_inner();
    let future = common::snapshots::create_shard_snapshot(
        dispatcher.toc(&access, &pass).clone(),
        access,
        collection,
        shard,
    );

    helpers::time_or_accept(future, query.wait.unwrap_or(true)).await
}

#[get("/collections/{collection}/shards/{shard}/snapshot")]
async fn stream_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId)>,
    ActixAccess(access): ActixAccess,
) -> Result<SnapshotStream, HttpError> {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard) = path.into_inner();
    Ok(common::snapshots::stream_shard_snapshot(
        dispatcher.toc(&access, &pass).clone(),
        access,
        collection,
        shard,
    )
    .await?)
}

// TODO: `PUT` (same as `recover_from_snapshot`) or `POST`!?
#[put("/collections/{collection}/shards/{shard}/snapshots/recover")]
async fn recover_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    http_client: web::Data<HttpClient>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshottingParam>,
    web::Json(request): web::Json<ShardSnapshotRecover>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let future = async move {
        let (collection, shard) = path.into_inner();

        common::snapshots::recover_shard_snapshot(
            dispatcher.toc(&access, &pass).clone(),
            access,
            collection,
            shard,
            request.location,
            request.priority.unwrap_or_default(),
            request.checksum,
            http_client.as_ref().clone(),
            request.api_key,
        )
        .await?;

        Ok(true)
    };

    helpers::time_or_accept(future, query.wait.unwrap_or(true)).await
}

// TODO: `POST` (same as `upload_snapshot`) or `PUT`!?
#[post("/collections/{collection}/shards/{shard}/snapshots/upload")]
async fn upload_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshotUploadingParam>,
    MultipartForm(form): MultipartForm<SnapshottingForm>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard) = path.into_inner();
    let SnapshotUploadingParam {
        wait,
        priority,
        checksum,
    } = query.into_inner();

    // - `recover_shard_snapshot_impl` is *not* cancel safe
    //   - but the task is *spawned* on the runtime and won't be cancelled, if request is cancelled

    let future = cancel::future::spawn_cancel_on_drop(move |cancel| async move {
        // TODO: Run this check before the multipart blob is uploaded
        let collection_pass = access
            .check_global_access(AccessRequirements::new().manage())?
            .issue_pass(&collection);

        if let Some(checksum) = checksum {
            let snapshot_checksum = hash_file(form.snapshot.file.path()).await?;
            if !hashes_equal(snapshot_checksum.as_str(), checksum.as_str()) {
                return Err(StorageError::checksum_mismatch(snapshot_checksum, checksum));
            }
        }

        let future = async {
            let collection = dispatcher
                .toc(&access, &pass)
                .get_collection(&collection_pass)
                .await?;
            collection.assert_shard_exists(shard).await?;

            Result::<_, StorageError>::Ok(collection)
        };

        let collection = cancel::future::cancel_on_token(cancel.clone(), future).await??;

        // `recover_shard_snapshot_impl` is *not* cancel safe
        common::snapshots::recover_shard_snapshot_impl(
            dispatcher.toc(&access, &pass),
            &collection,
            shard,
            form.snapshot.file.path(),
            priority.unwrap_or_default(),
            cancel,
        )
        .await?;

        Ok(())
    })
    .map(|x| x.map_err(Into::into).and_then(|x| x));

    helpers::time_or_accept(future, wait.unwrap_or(true)).await
}

#[get("/collections/{collection}/shards/{shard}/snapshots/{snapshot}")]
async fn download_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId, String)>,
    ActixAccess(access): ActixAccess,
) -> Result<impl Responder, HttpError> {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard, snapshot) = path.into_inner();
    let collection_pass =
        access.check_collection_access(&collection, AccessRequirements::new().whole().extras())?;
    let collection = dispatcher
        .toc(&access, &pass)
        .get_collection(&collection_pass)
        .await?;
    let snapshots_storage_manager = collection.get_snapshots_storage_manager()?;
    let snapshot_path = collection
        .shards_holder()
        .read()
        .await
        .get_shard_snapshot_path(collection.snapshots_path(), shard, &snapshot)
        .await?;
    let snapshot_stream = snapshots_storage_manager
        .get_snapshot_stream(&snapshot_path)
        .await?;
    Ok(snapshot_stream)
}

#[delete("/collections/{collection}/shards/{shard}/snapshots/{snapshot}")]
async fn delete_shard_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId, String)>,
    query: web::Query<SnapshottingParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // nothing to verify.
    let pass = new_unchecked_verification_pass();

    let (collection, shard, snapshot) = path.into_inner();
    let future = common::snapshots::delete_shard_snapshot(
        dispatcher.toc(&access, &pass).clone(),
        access,
        collection,
        shard,
        snapshot,
    )
    .map_ok(|_| true)
    .map_err(Into::into);

    helpers::time_or_accept(future, query.wait.unwrap_or(true)).await
}

// Configure services
pub fn config_snapshots_api(cfg: &mut web::ServiceConfig) {
    cfg.service(list_snapshots)
        .service(create_snapshot)
        .service(upload_snapshot)
        .service(recover_from_snapshot)
        .service(get_snapshot)
        .service(list_full_snapshots)
        .service(create_full_snapshot)
        .service(get_full_snapshot)
        .service(delete_full_snapshot)
        .service(delete_collection_snapshot)
        .service(list_shard_snapshots)
        .service(create_shard_snapshot)
        .service(stream_shard_snapshot)
        .service(recover_shard_snapshot)
        .service(upload_shard_snapshot)
        .service(download_shard_snapshot)
        .service(delete_shard_snapshot);
}
