use std::collections::HashMap;

use actix_web::{HttpResponse, get, put, web};
use actix_web_validator::{Json, Query};
use api::grpc::qdrant::GetQuotaUsageRequest;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::channel_service::ChannelService;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use shard::PeerId;
use storage::dispatcher::Dispatcher;
use storage::quota::{PeerQuotaUsage, QuotaConfig, QuotaUsage};
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct QuotaParams {
    /// Wait until the new quota config is confirmed by consensus on this peer.
    #[serde(default)]
    pub wait: bool,
}

#[get("/quotas")]
async fn get_quotas(dispatcher: web::Data<Dispatcher>, ActixAuth(auth): ActixAuth) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new(), "get_quotas")?;

        let toc = dispatcher.toc(&auth, &pass);
        let mut status = toc.quota_manager().status();

        if toc.is_distributed() {
            status.peers = Some(collect_peer_usage(toc.get_channel_service()).await);
        }

        Ok(status)
    })
    .await
}

/// Ask every known peer what it is using, in parallel.
///
/// A peer that does not answer is left out rather than failing the request: the
/// point of the call is to find the node that is out of room, and it is exactly
/// the struggling nodes that are most likely to time out.
async fn collect_peer_usage(channel_service: &ChannelService) -> HashMap<PeerId, PeerQuotaUsage> {
    let peers: Vec<_> = channel_service
        .id_to_address
        .read()
        .keys()
        .copied()
        .collect();

    let mut requests = peers
        .into_iter()
        .map(|peer_id| {
            channel_service
                .with_qdrant_client(peer_id, |mut client| async move {
                    client.get_quota_usage(GetQuotaUsageRequest {}).await
                })
                .map(move |result| (peer_id, result))
        })
        .collect::<FuturesUnordered<_>>();

    let mut usage = HashMap::new();

    while let Some((peer_id, result)) = requests.next().await {
        match result {
            Ok(response) => {
                let Some(peer) = response.into_inner().result else {
                    log::warn!("Peer {peer_id} answered the quota usage request with no usage");
                    continue;
                };

                usage.insert(
                    peer_id,
                    PeerQuotaUsage {
                        usage: QuotaUsage {
                            // Percentages, so they fit a `u8` unless the peer is
                            // reporting nonsense; clamp rather than wrap.
                            resident_memory_percent: peer
                                .resident_memory_percent
                                .map(|percent| percent.min(100) as u8),
                            disk_usage_percent: peer
                                .disk_usage_percent
                                .map(|percent| percent.min(100) as u8),
                        },
                        exceeded: peer.exceeded,
                    },
                );
            }
            Err(err) => {
                log::warn!("Failed to read quota usage from peer {peer_id}: {err}");
            }
        }
    }

    usage
}

#[put("/quotas")]
async fn update_quotas(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    Query(params): Query<QuotaParams>,
    Json(config): Json<QuotaConfig>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "update_quotas")?;

        dispatcher
            .toc(&auth, &pass)
            .update_quota_config(config, params.wait)
            .await?;

        Ok(true)
    })
    .await
}

pub fn config_quota_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_quotas).service(update_quotas);
}
