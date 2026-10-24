use std::sync::Arc;
use std::time::Instant;

use api::grpc::qdrant::quotas_server::Quotas;
use api::grpc::qdrant::{
    GetQuotasRequest, GetQuotasResponse, QuotaConfig, QuotaResourceUsage, QuotaStatus, QuotaUsage,
    UpdateQuotasRequest, UpdateQuotasResponse,
};
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status, async_trait};

use super::validate;
use crate::common::quotas::{get_quota_status, update_quota_status};
use crate::tonic::auth::extract_auth;

pub struct QuotaService {
    dispatcher: Arc<Dispatcher>,
}

impl QuotaService {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[async_trait]
impl Quotas for QuotaService {
    async fn get(
        &self,
        mut request: Request<GetQuotasRequest>,
    ) -> Result<Response<GetQuotasResponse>, Status> {
        let auth = extract_auth(&mut request);
        let timing = Instant::now();

        let status = get_quota_status(&self.dispatcher, &auth).await?;

        let response = GetQuotasResponse {
            result: Some(quota_status_to_grpc(status)),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }

    async fn update(
        &self,
        mut request: Request<UpdateQuotasRequest>,
    ) -> Result<Response<UpdateQuotasResponse>, Status> {
        validate(request.get_ref())?;

        let auth = extract_auth(&mut request);
        let timing = Instant::now();

        let UpdateQuotasRequest { config, wait } = request.into_inner();

        let config = quota_config_from_grpc(require_config(config)?);

        update_quota_status(&self.dispatcher, &auth, config, wait).await?;

        let response = UpdateQuotasResponse {
            result: true,
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }
}

fn quota_status_to_grpc(status: storage::quota::QuotaStatus) -> QuotaStatus {
    QuotaStatus {
        config: Some(quota_config_to_grpc(status.config)),
        usage: Some(QuotaResourceUsage {
            resident_memory_percent: status.usage.resident_memory_percent.map(u32::from),
            disk_usage_percent: status.usage.disk_usage_percent.map(u32::from),
        }),
        peers: status
            .peers
            .unwrap_or_default()
            .into_iter()
            .map(|(peer_id, peer)| {
                let usage = QuotaUsage {
                    resident_memory_percent: peer.usage.resident_memory_percent.map(u32::from),
                    disk_usage_percent: peer.usage.disk_usage_percent.map(u32::from),
                    exceeded: peer.exceeded,
                };
                (peer_id, usage)
            })
            .collect(),
    }
}

fn quota_config_to_grpc(config: storage::quota::QuotaConfig) -> QuotaConfig {
    QuotaConfig {
        enabled: config.enabled,
        max_resident_memory_percent: config.max_resident_memory_percent.map(u32::from),
        max_disk_usage_percent: config.max_disk_usage_percent.map(u32::from),
        release_margin_percent: config.release_margin_percent.map(u32::from),
    }
}

/// An absent `config` is rejected rather than defaulted: an empty config would
/// silently disable the quotas for a client that forgot to set the field.
fn require_config(config: Option<QuotaConfig>) -> Result<QuotaConfig, Status> {
    config.ok_or_else(|| Status::invalid_argument("Missing quota config"))
}

/// `config`'s percent fields are already range-checked to fit in `0..=100` by
/// `UpdateQuotasRequest`'s generated `Validate` impl (see `lib/api/build.rs`'s
/// `quota_service.proto` validation block), which the caller runs before this
/// is reached, so truncating `u32` -> `u8` here can't lose information.
fn quota_config_from_grpc(config: QuotaConfig) -> storage::quota::QuotaConfig {
    storage::quota::QuotaConfig {
        enabled: config.enabled,
        max_resident_memory_percent: config.max_resident_memory_percent.map(|v| v as u8),
        max_disk_usage_percent: config.max_disk_usage_percent.map(|v| v as u8),
        release_margin_percent: config.release_margin_percent.map(|v| v as u8),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use storage::quota;

    use super::*;

    fn sample_config() -> quota::QuotaConfig {
        quota::QuotaConfig {
            enabled: true,
            max_resident_memory_percent: Some(80),
            max_disk_usage_percent: Some(90),
            release_margin_percent: Some(5),
        }
    }

    #[test]
    fn test_quota_config_grpc_round_trip() {
        let original = sample_config();

        let grpc = quota_config_to_grpc(original);
        assert_eq!(grpc.enabled, original.enabled);
        assert_eq!(grpc.max_resident_memory_percent, Some(80));
        assert_eq!(grpc.max_disk_usage_percent, Some(90));
        assert_eq!(grpc.release_margin_percent, Some(5));

        let round_tripped = quota_config_from_grpc(grpc);
        assert_eq!(round_tripped, original);
    }

    #[test]
    fn test_quota_config_grpc_round_trip_all_unset() {
        let original = quota::QuotaConfig {
            enabled: false,
            max_resident_memory_percent: None,
            max_disk_usage_percent: None,
            release_margin_percent: None,
        };

        let grpc = quota_config_to_grpc(original);
        assert!(!grpc.enabled);
        assert_eq!(grpc.max_resident_memory_percent, None);
        assert_eq!(grpc.max_disk_usage_percent, None);
        assert_eq!(grpc.release_margin_percent, None);

        assert_eq!(quota_config_from_grpc(grpc), original);
    }

    #[test]
    fn test_update_rejects_missing_config() {
        let err = require_config(None).expect_err("a missing config must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let all_unset = QuotaConfig {
            enabled: false,
            max_resident_memory_percent: None,
            max_disk_usage_percent: None,
            release_margin_percent: None,
        };
        assert!(!require_config(Some(all_unset)).unwrap().enabled);
    }

    #[test]
    fn test_quota_status_to_grpc_without_peers() {
        let status = quota::QuotaStatus {
            config: sample_config(),
            usage: quota::QuotaUsage {
                resident_memory_percent: Some(42),
                disk_usage_percent: None,
            },
            peers: None,
        };

        let grpc = quota_status_to_grpc(status);

        let usage = grpc.usage.expect("usage must be set");
        assert_eq!(usage.resident_memory_percent, Some(42));
        assert_eq!(usage.disk_usage_percent, None);
        assert!(
            grpc.peers.is_empty(),
            "no peers were reported, the map must be empty",
        );
    }

    #[test]
    fn test_quota_status_to_grpc_with_peers() {
        let mut peers = HashMap::new();
        peers.insert(
            7u64,
            quota::PeerQuotaUsage {
                usage: quota::QuotaUsage {
                    resident_memory_percent: Some(55),
                    disk_usage_percent: Some(60),
                },
                exceeded: true,
            },
        );

        let status = quota::QuotaStatus {
            config: sample_config(),
            usage: quota::QuotaUsage {
                resident_memory_percent: None,
                disk_usage_percent: None,
            },
            peers: Some(peers),
        };

        let grpc = quota_status_to_grpc(status);

        assert_eq!(grpc.peers.len(), 1);
        let peer = grpc.peers.get(&7).expect("peer 7 must be present");
        assert_eq!(peer.resident_memory_percent, Some(55));
        assert_eq!(peer.disk_usage_percent, Some(60));
        assert!(peer.exceeded);
    }
}
