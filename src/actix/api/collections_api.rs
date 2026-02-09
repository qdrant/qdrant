use std::time::Duration;

use actix_web::rt::time::Instant;
use actix_web::{HttpResponse, Responder, delete, get, patch, post, put, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::cluster_ops::ClusterOperations;
use collection::operations::types::{CollectionError, OptimizationsRequestOptions};
use collection::operations::verification::new_unchecked_verification_pass;
use serde::Deserialize;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection, UpdateCollectionOperation,
};
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use super::CollectionPath;
use crate::actix::api::StrictCollectionPath;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{self, process_response};
use crate::common::collections::*;

#[derive(Debug, Deserialize, Validate)]
pub struct WaitTimeout {
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

impl WaitTimeout {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/collections")]
async fn get_collections(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_list_collections(dispatcher.toc(&auth, &pass), &auth)).await
}

#[get("/aliases")]
async fn get_aliases(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_list_aliases(dispatcher.toc(&auth, &pass), &auth)).await
}

#[get("/collections/{name}")]
async fn get_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_get_collection(
        dispatcher.toc(&auth, &pass),
        &auth,
        &collection.name,
        None,
    ))
    .await
}

#[get("/collections/{name}/exists")]
async fn get_collection_existence(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_collection_exists(
        dispatcher.toc(&auth, &pass),
        &auth,
        &collection.name,
    ))
    .await
}

#[get("/collections/{name}/aliases")]
async fn get_collection_aliases(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_list_collection_aliases(
        dispatcher.toc(&auth, &pass),
        &auth,
        &collection.name,
    ))
    .await
}

#[put("/collections/{name}")]
async fn create_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<StrictCollectionPath>,
    operation: Json<CreateCollection>,
    Query(query): Query<WaitTimeout>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    let timing = Instant::now();
    let create_collection_op =
        CreateCollectionOperation::new(collection.name.clone(), operation.into_inner());

    let Ok(create_collection_op) = create_collection_op else {
        return process_response(create_collection_op, timing, None);
    };

    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::CreateCollection(create_collection_op),
            auth,
            query.timeout(),
        )
        .await;
    process_response(response, timing, None)
}

#[patch("/collections/{name}")]
async fn update_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateCollection>,
    Query(query): Query<WaitTimeout>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::UpdateCollection(UpdateCollectionOperation::new(
                name,
                operation.into_inner(),
            )),
            auth,
            query.timeout(),
        )
        .await;
    process_response(response, timing, None)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    Query(query): Query<WaitTimeout>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(
                collection.name.clone(),
            )),
            auth,
            query.timeout(),
        )
        .await;
    process_response(response, timing, None)
}

#[post("/collections/aliases")]
async fn update_aliases(
    dispatcher: web::Data<Dispatcher>,
    operation: Json<ChangeAliasesOperation>,
    Query(query): Query<WaitTimeout>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::ChangeAliases(operation.0),
            auth,
            query.timeout(),
        )
        .await;
    process_response(response, timing, None)
}

#[get("/collections/{name}/cluster")]
async fn get_cluster_info(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    // No request to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_get_collection_cluster(
        dispatcher.toc(&auth, &pass),
        &auth,
        &collection.name,
    ))
    .await
}

#[post("/collections/{name}/cluster")]
async fn update_collection_cluster(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<ClusterOperations>,
    Query(query): Query<WaitTimeout>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let timing = Instant::now();
    let wait_timeout = query.timeout();
    let response = do_update_collection_cluster(
        &dispatcher.into_inner(),
        collection.name.clone(),
        operation.0,
        auth,
        wait_timeout,
    )
    .await;
    process_response(response, timing, None)
}

#[derive(Deserialize, Clone, Validate)]
struct OptimizationsParam {
    with: Option<String>,
    completed_limit: Option<u64>,
}

const DEFAULT_OPTIMIZATIONS_COMPLETED_LIMIT: u64 = 16;

impl TryFrom<&OptimizationsParam> for OptimizationsRequestOptions {
    type Error = CollectionError;

    fn try_from(
        params: &OptimizationsParam,
    ) -> Result<OptimizationsRequestOptions, CollectionError> {
        let OptimizationsParam {
            with,
            completed_limit,
        } = params;
        let completed_limit =
            completed_limit.unwrap_or(DEFAULT_OPTIMIZATIONS_COMPLETED_LIMIT) as usize;
        let mut options = OptimizationsRequestOptions {
            queued: false,
            completed_limit: None,
            idle_segments: false,
        };
        for field in with.as_deref().unwrap_or("").split(',') {
            match field.trim() {
                "" => (),
                "queued" => options.queued = true,
                "completed" => options.completed_limit = Some(completed_limit),
                "idle_segments" => options.idle_segments = true,
                _ => Err(CollectionError::bad_input(format!(
                    "Unknown field in 'with' parameter: {field}"
                )))?,
            }
        }
        Ok(options)
    }
}

#[get("/collections/{name}/optimizations")]
fn get_optimizations(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAuth(auth): ActixAuth,
    params: Query<OptimizationsParam>,
) -> impl Future<Output = HttpResponse> {
    helpers::time(async move {
        let options = OptimizationsRequestOptions::try_from(&params.into_inner())?;
        let pass = new_unchecked_verification_pass();
        let collection_pass = auth.check_collection_access(
            &collection.name,
            AccessRequirements::new(),
            "get_optimizations",
        )?;
        Ok(dispatcher
            .toc(&auth, &pass)
            .get_collection(&collection_pass)
            .await?
            .optimizations(options)
            .await?)
    })
}

// Configure services
pub fn config_collections_api(cfg: &mut web::ServiceConfig) {
    // Ordering of services is important for correct path pattern matching
    // See: <https://github.com/qdrant/qdrant/issues/3543>
    cfg.service(update_aliases)
        .service(get_collections)
        .service(get_collection)
        .service(get_collection_existence)
        .service(create_collection)
        .service(update_collection)
        .service(delete_collection)
        .service(get_aliases)
        .service(get_collection_aliases)
        .service(get_cluster_info)
        .service(get_optimizations)
        .service(update_collection_cluster);
}

#[cfg(test)]
mod tests {
    use actix_web::web::Query;

    use super::WaitTimeout;

    #[test]
    fn timeout_is_deserialized() {
        let timeout: WaitTimeout = Query::from_query("").unwrap().0;
        assert!(timeout.timeout.is_none());
        let timeout: WaitTimeout = Query::from_query("timeout=10").unwrap().0;
        assert_eq!(timeout.timeout, Some(10))
    }
}
