use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{FacetRequest, FacetResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::read_params::ReadParams;
use crate::actix::api::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{process_response, process_response_error};

#[post("/collections/{name}/facet")]
async fn facet(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<FacetRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let FacetRequest {
        facet_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(&facet_request, &collection.name, &dispatcher, &access).await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, timing),
    };

    let facet_params = From::from(facet_request);

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = dispatcher
        .toc_new(&access, &pass)
        .facet(
            &collection.name,
            facet_params,
            shard_selection,
            params.consistency,
            access,
            params.timeout(),
        )
        .await
        .map(FacetResponse::from);

    process_response(response, timing)
}

pub fn config_facet_api(cfg: &mut web::ServiceConfig) {
    cfg.service(facet);
}
