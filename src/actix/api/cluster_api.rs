use actix_web::rt::time::Instant;
use actix_web::{delete, get, post, web, Responder};
use actix_web_validator::Query;
use serde::Deserialize;
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use crate::actix::helpers::{self, process_response};

#[derive(Debug, Deserialize, Validate)]
struct QueryParams {
    #[serde(default)]
    force: bool,
    #[serde(default)]
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

#[get("/cluster")]
async fn cluster_status(dispatcher: web::Data<Dispatcher>) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher.cluster_status();
    process_response(Ok(response), timing)
}

#[post("/cluster/recover")]
async fn recover_current_peer(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    process_response(toc.request_snapshot().map(|_| true), timing)
}

#[delete("/cluster/peer/{peer_id}")]
async fn remove_peer(
    dispatcher: web::Data<Dispatcher>,
    peer_id: web::Path<u64>,
    Query(params): Query<QueryParams>,
) -> impl Responder {
    helpers::time(async move {
        let dispatcher = dispatcher.into_inner();
        let peer_id = peer_id.into_inner();

        let has_shards = dispatcher.peer_has_shards(peer_id).await;
        if !params.force && has_shards {
            return Err(StorageError::BadRequest {
                description: format!("Cannot remove peer {peer_id} as there are shards on it"),
            }
            .into());
        }

        let res = match dispatcher.consensus_state() {
            Some(consensus_state) => {
                consensus_state
                    .propose_consensus_op_with_await(
                        ConsensusOperations::RemovePeer(peer_id),
                        params.timeout.map(std::time::Duration::from_secs),
                    )
                    .await
            }
            None => Err(StorageError::BadRequest {
                description: "Distributed mode disabled.".to_string(),
            }),
        };
        Ok(res?)
    })
    .await
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status)
        .service(remove_peer)
        .service(recover_current_peer);
}
