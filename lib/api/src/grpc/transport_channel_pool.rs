use std::collections::HashMap;
use std::time::Duration;
use std::vec::Vec;

use rand::seq::SliceRandom;
use tonic::transport::{Channel, Error, Uri};

/// Holds a pool of channels established for a set of URIs.
/// Channel are shared by cloning them.
/// Make the `pool_size` larger to increase throughput.
#[derive(Default)]
pub struct TransportChannelPool {
    uri_to_pool: tokio::sync::RwLock<HashMap<Uri, Vec<Channel>>>,
    pool_size: usize,
    grpc_timeout: Duration,
}

impl TransportChannelPool {
    pub fn new(p2p_grpc_timeout: Duration, pool_size: usize) -> Self {
        Self {
            uri_to_pool: Default::default(),
            grpc_timeout: p2p_grpc_timeout,
            pool_size,
        }
    }

    pub async fn make_channel(grpc_timeout: Duration, uri: Uri) -> Result<Channel, Error> {
        let endpoint = Channel::builder(uri)
            .timeout(grpc_timeout)
            .connect_timeout(grpc_timeout)
            .keep_alive_while_idle(true);
        // `connect` is using the `Reconnect` network service internally to handle dropped connections
        endpoint.connect().await
    }

    /// Initialize a pool for the URI and return a clone of the first channel.
    /// Does not fail if the pool already exist.
    async fn init_pool_for_uri(&self, uri: Uri) -> Result<Channel, Error> {
        let mut guard = self.uri_to_pool.write().await;
        match guard.get(&uri) {
            None => {
                let mut channels = Vec::with_capacity(self.pool_size);
                for _ in 0..self.pool_size {
                    let channel = Self::make_channel(self.grpc_timeout, uri.clone()).await?;
                    channels.push(channel);
                }
                let result = channels[0].clone();
                guard.insert(uri.clone(), channels);
                Ok(result)
            }
            Some(channels) => Ok(channels[0].clone()),
        }
    }

    pub async fn get_pooled_channel(&self, uri: &Uri) -> Option<Channel> {
        let guard = self.uri_to_pool.read().await;
        guard
            .get(uri)
            .and_then(|channels| channels.choose(&mut rand::thread_rng()))
            .cloned()
    }

    pub async fn get_or_create_pooled_channel(&self, uri: &Uri) -> Result<Channel, Error> {
        match self.get_pooled_channel(uri).await {
            None => self.init_pool_for_uri(uri.clone()).await,
            Some(channel) => Ok(channel),
        }
    }
}
