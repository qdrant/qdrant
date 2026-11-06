use std::time::Duration;

use parking_lot::Mutex;
use tonic::transport::{Channel, ClientTlsConfig, Error as TonicError, Uri};

use crate::grpc::dynamic_pool::{CountedItem, DynamicPool};

pub async fn make_grpc_channel(
    timeout: Duration,
    connection_timeout: Duration,
    uri: Uri,
    tls_config: Option<ClientTlsConfig>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
) -> Result<Channel, TonicError> {
    let mut endpoint = Channel::builder(uri)
        .timeout(timeout)
        .connect_timeout(connection_timeout)
        .http2_max_local_error_reset_streams(None)
        // Keep idle pooled channels alive across stateful network layers (NAT, eBPF
        // conntrack, load balancers) that otherwise expire them silently.
        // See: <https://github.com/qdrant/qdrant/issues/11271>
        .http2_keep_alive_interval(keepalive_interval)
        .keep_alive_timeout(keepalive_timeout)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(keepalive_interval));

    if let Some(config) = tls_config {
        endpoint = endpoint.tls_config(config)?;
    }
    // `connect` is using the `Reconnect` network service internally to handle dropped connections
    endpoint.connect().await
}

pub struct DynamicChannelPool {
    pool: Mutex<DynamicPool<Channel>>,
    uri: Uri,
    timeout: Duration,
    connection_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
}

impl DynamicChannelPool {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        uri: Uri,
        timeout: Duration,
        connection_timeout: Duration,
        tls_config: Option<ClientTlsConfig>,
        usage_per_channel: usize,
        min_channels: usize,
        keepalive_interval: Duration,
        keepalive_timeout: Duration,
    ) -> Result<Self, TonicError> {
        let mut channels = Vec::with_capacity(min_channels);
        for _ in 0..min_channels {
            let channel = make_grpc_channel(
                timeout,
                connection_timeout,
                uri.clone(),
                tls_config.clone(),
                keepalive_interval,
                keepalive_timeout,
            )
            .await?;
            channels.push(channel);
        }

        let pool = DynamicPool::new(channels, usage_per_channel, min_channels);
        Ok(Self {
            pool: Mutex::new(pool),
            uri,
            timeout,
            connection_timeout,
            tls_config,
            keepalive_interval,
            keepalive_timeout,
        })
    }

    pub async fn choose(&self) -> Result<CountedItem<Channel>, TonicError> {
        let channel = self.pool.lock().choose();
        let channel = match channel {
            None => {
                let channel = make_grpc_channel(
                    self.timeout,
                    self.connection_timeout,
                    self.uri.clone(),
                    self.tls_config.clone(),
                    self.keepalive_interval,
                    self.keepalive_timeout,
                )
                .await?;
                self.pool.lock().add(channel)
            }
            Some(channel) => channel,
        };
        Ok(channel)
    }

    pub fn drop_channel(&self, channel: CountedItem<Channel>) {
        self.pool.lock().drop_item(channel);
    }
}
