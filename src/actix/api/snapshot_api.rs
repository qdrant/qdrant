use std::fs::File;

use actix_files::NamedFile;
use actix_multipart::form::tempfile::TempFile;
use actix_multipart::form::MultipartForm;
use actix_web::rt::time::Instant;
use actix_web::{delete, get, post, put, web, HttpRequest, Responder, Result};
use actix_web_validator as valid;
use collection::common::sha_256::{hash_file, hashes_equal};
use collection::operations::snapshot_ops::{
    ShardSnapshotRecover, SnapshotPriority, SnapshotRecover,
};
use collection::shards::shard::ShardId;
use futures::{FutureExt as _, TryFutureExt as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snapshot_manager::file::SnapshotFile;
use storage::content_manager::snapshots::do_create_full_snapshot;
use storage::content_manager::snapshots::recover::do_recover_from_snapshot;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::helpers::{
    self, accepted_response, process_response, snapshot_manager_into_actix_error,
};
use crate::common;
use crate::common::collections::*;
use crate::common::http_client::HttpClient;

#[derive(Deserialize, Validate)]
struct SnapshotPath {
    #[serde(rename = "snapshot_name")]
    #[validate(length(min = 1))]
    name: String,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct SnapshotUploadingParam {
    pub wait: Option<bool>,
    pub priority: Option<SnapshotPriority>,

    /// Optional SHA256 checksum to verify snapshot integrity before recovery.
    #[serde(default)]
    #[validate(custom = "::common::validation::validate_sha256_hash")]
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
pub async fn do_get_snapshot(
    toc: &TableOfContent,
    snapshot: SnapshotFile,
    req: &HttpRequest,
) -> Result<impl Responder> {
    let (path, _temp) = toc
        .snapshot_manager
        .get_snapshot_path(&snapshot)
        .await
        .map_err(snapshot_manager_into_actix_error)?;

    let file = File::open(path).map_err(|x| snapshot_manager_into_actix_error(x.into()))?;

    let file = NamedFile::from_file(file, snapshot.get_path("./"));

    // Need to pre-generate response so that TempPath doesn't drop the file before
    // the response can be generated.
    let res = file.respond_to(req);
    Ok(res)
}

#[get("/collections/{name}/snapshots")]
async fn list_snapshots(toc: web::Data<TableOfContent>, path: web::Path<String>) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response = do_list_snapshots(&toc, &collection_name).await;
    process_response(response, timing)
}

#[post("/collections/{name}/snapshots")]
async fn create_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    params: valid::Query<SnapshottingParam>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let wait = params.wait.unwrap_or(true);

    let timing = Instant::now();
    let response = do_create_snapshot(dispatcher.get_ref(), &collection_name, wait).await;
    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[post("/collections/{name}/snapshots/upload")]
async fn upload_snapshot(
    dispatcher: web::Data<Dispatcher>,
    http_client: web::Data<HttpClient>,
    collection: valid::Path<CollectionPath>,
    MultipartForm(form): MultipartForm<SnapshottingForm>,
    params: valid::Query<SnapshotUploadingParam>,
) -> impl Responder {
    let timing = Instant::now();
    let snapshot = form.snapshot;
    let wait = params.wait.unwrap_or(true);

    if let Some(checksum) = &params.checksum {
        let snapshot_checksum = match hash_file(snapshot.file.path()).await {
            Ok(checksum) => checksum,
            Err(err) => return process_response::<()>(Err(err.into()), timing),
        };
        if !hashes_equal(snapshot_checksum.as_str(), checksum.as_str()) {
            return process_response::<()>(
                Err(StorageError::checksum_mismatch(snapshot_checksum, checksum)),
                timing,
            );
        }
    }

    let snapshot_location = match dispatcher
        .snapshot_manager
        .do_save_uploaded_snapshot(&collection.name, snapshot.file_name, snapshot.file)
        .await
    {
        Ok(location) => location,
        Err(err) => return process_response::<()>(Err(err.into()), timing),
    };

    let http_client = match http_client.client() {
        Ok(http_client) => http_client,
        Err(err) => return process_response::<()>(Err(err.into()), timing),
    };

    let snapshot_recover = SnapshotRecover {
        location: snapshot_location,
        priority: params.priority,
        checksum: None,
    };

    let response = do_recover_from_snapshot(
        dispatcher.get_ref(),
        &collection.name,
        snapshot_recover,
        wait,
        http_client,
    )
    .await;

    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[put("/collections/{name}/snapshots/recover")]
async fn recover_from_snapshot(
    dispatcher: web::Data<Dispatcher>,
    http_client: web::Data<HttpClient>,
    collection: valid::Path<CollectionPath>,
    request: valid::Json<SnapshotRecover>,
    params: valid::Query<SnapshottingParam>,
) -> impl Responder {
    let timing = Instant::now();
    let snapshot_recover = request.into_inner();
    let wait = params.wait.unwrap_or(true);

    let http_client = match http_client.client() {
        Ok(http_client) => http_client,
        Err(err) => return process_response::<()>(Err(err.into()), timing),
    };

    let response = do_recover_from_snapshot(
        dispatcher.get_ref(),
        &collection.name,
        snapshot_recover,
        wait,
        http_client,
    )
    .await;

    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[get("/collections/{name}/snapshots/{snapshot_name}")]
async fn get_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, String)>,
    req: HttpRequest,
) -> impl Responder {
    let (collection_name, snapshot_name) = path.into_inner();
    let snapshot = SnapshotFile::new_collection(snapshot_name, collection_name);
    do_get_snapshot(&toc, snapshot, &req).await
}
#[get("/snapshots")]
async fn list_full_snapshots(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let response = toc.snapshot_manager.do_list_full_snapshots().await;
    process_response(response.map_err(|x| x.into()), timing)
}

#[post("/snapshots")]
async fn create_full_snapshot(
    dispatcher: web::Data<Dispatcher>,
    params: valid::Query<SnapshottingParam>,
) -> impl Responder {
    let timing = Instant::now();
    let wait = params.wait.unwrap_or(true);
    let response = do_create_full_snapshot(dispatcher.get_ref(), wait).await;

    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[get("/snapshots/{snapshot_name}")]
async fn get_full_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    req: HttpRequest,
) -> impl Responder {
    let snapshot = SnapshotFile::new_full(path.into_inner());
    do_get_snapshot(&toc, snapshot, &req).await
}

#[delete("/snapshots/{snapshot_name}")]
async fn delete_full_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<String>,
    params: valid::Query<SnapshottingParam>,
) -> impl Responder {
    let snapshot = SnapshotFile::new_full(path.into_inner());
    let timing = Instant::now();
    let wait = params.wait.unwrap_or(true);

    let response = dispatcher
        .snapshot_manager
        .do_delete_snapshot(&snapshot, wait)
        .await
        .map_err(|x| x.into());

    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[delete("/collections/{name}/snapshots/{snapshot_name}")]
async fn delete_collection_snapshot(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, String)>,
    params: valid::Query<SnapshottingParam>,
) -> impl Responder {
    let (collection_name, snapshot_name) = path.into_inner();
    let snapshot = SnapshotFile::new_collection(snapshot_name, collection_name);
    let timing = Instant::now();
    let wait = params.wait.unwrap_or(true);
    let response = dispatcher
        .snapshot_manager
        .do_delete_snapshot(&snapshot, wait)
        .await
        .map_err(|x| x.into());
    match response {
        Err(_) => process_response(response, timing),
        Ok(_) if wait => process_response(response, timing),
        Ok(_) => accepted_response(timing),
    }
}

#[get("/collections/{collection}/shards/{shard}/snapshots")]
async fn list_shard_snapshots(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, ShardId)>,
) -> impl Responder {
    let (collection, shard) = path.into_inner();
    let future = common::snapshots::list_shard_snapshots(toc.into_inner(), collection, shard)
        .map_err(Into::into);

    helpers::time(future).await
}

#[post("/collections/{collection}/shards/{shard}/snapshots")]
async fn create_shard_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshottingParam>,
) -> impl Responder {
    let (collection, shard) = path.into_inner();
    let future = common::snapshots::create_shard_snapshot(toc.into_inner(), collection, shard)
        .map_err(Into::into);

    helpers::time_or_accept(future, query.wait.unwrap_or(true)).await
}

// TODO: `PUT` (same as `recover_from_snapshot`) or `POST`!?
#[put("/collections/{collection}/shards/{shard}/snapshots/recover")]
async fn recover_shard_snapshot(
    toc: web::Data<TableOfContent>,
    http_client: web::Data<HttpClient>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshottingParam>,
    web::Json(request): web::Json<ShardSnapshotRecover>,
) -> impl Responder {
    let snapshot_manager = toc.snapshot_manager.clone();
    let future = async move {
        let (collection, shard) = path.into_inner();

        common::snapshots::recover_shard_snapshot(
            toc.into_inner(),
            collection,
            shard,
            snapshot_manager,
            request.location,
            request.priority.unwrap_or_default(),
            request.checksum,
            http_client.as_ref().clone(),
        )
        .await?;

        Ok(true)
    };

    helpers::time_or_accept(future, query.wait.unwrap_or(true)).await
}

// TODO: `POST` (same as `upload_snapshot`) or `PUT`!?
#[post("/collections/{collection}/shards/{shard}/snapshots/upload")]
async fn upload_shard_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, ShardId)>,
    query: web::Query<SnapshotUploadingParam>,
    MultipartForm(form): MultipartForm<SnapshottingForm>,
) -> impl Responder {
    let (collection, shard) = path.into_inner();
    let SnapshotUploadingParam {
        wait,
        priority,
        checksum,
    } = query.into_inner();

    // - `recover_shard_snapshot_impl` is *not* cancel safe
    //   - but the task is *spawned* on the runtime and won't be cancelled, if request is cancelled

    let future = cancel::future::spawn_cancel_on_drop(move |cancel| async move {
        if let Some(checksum) = checksum {
            let snapshot_checksum = hash_file(form.snapshot.file.path()).await?;
            if !hashes_equal(snapshot_checksum.as_str(), checksum.as_str()) {
                let err = StorageError::checksum_mismatch(snapshot_checksum, checksum);
                return Result::<_, helpers::HttpError>::Err(err.into());
            }
        }

        let future = async {
            let collection = toc.get_collection(&collection).await?;
            collection.assert_shard_exists(shard).await?;

            Result::<_, helpers::HttpError>::Ok(collection)
        };

        let collection = cancel::future::cancel_on_token(cancel.clone(), future).await??;

        // `recover_shard_snapshot_impl` is *not* cancel safe
        common::snapshots::recover_shard_snapshot_impl(
            &toc,
            &collection,
            shard,
            form.snapshot.file.path(),
            priority.unwrap_or_default(),
            cancel,
        )
        .await?;

        Result::<_, helpers::HttpError>::Ok(())
    })
    .map_err(Into::into)
    .map(|res| res.and_then(|res| res));

    helpers::time_or_accept(future, wait.unwrap_or(true)).await
}

#[get("/collections/{collection}/shards/{shard}/snapshots/{snapshot}")]
async fn download_shard_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, ShardId, String)>,
    req: HttpRequest,
) -> impl Responder {
    let (collection, shard, snapshot) = path.into_inner();
    let snapshot = SnapshotFile::new_shard(snapshot, collection, shard);
    do_get_snapshot(&toc, snapshot, &req).await
}

#[delete("/collections/{collection}/shards/{shard}/snapshots/{snapshot}")]
async fn delete_shard_snapshot(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, ShardId, String)>,
    query: web::Query<SnapshottingParam>,
) -> impl Responder {
    let (collection, shard, snapshot) = path.into_inner();
    let future =
        common::snapshots::delete_shard_snapshot(toc.into_inner(), collection, shard, snapshot)
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
        .service(recover_shard_snapshot)
        .service(upload_shard_snapshot)
        .service(download_shard_snapshot)
        .service(delete_shard_snapshot);
}
