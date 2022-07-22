use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;
use std::vec::Vec;
use std::{collections::HashMap, time::Instant};

use rand::seq::SliceRandom;
use tonic::transport::{Channel, Error as TonicError, Uri};

const CHANNEL_TTL: Duration = Duration::from_secs(5);

struct ChannelsToUri {
    channels: Vec<Channel>,
    init_at: Instant,
}

impl ChannelsToUri {
    async fn init(
        uri: Uri,
        pool_size: NonZeroUsize,
        grpc_timeout: Duration,
    ) -> Result<Self, TonicError> {
        let mut channels = Vec::with_capacity(pool_size.into());
        for _ in 0..pool_size.into() {
            let channel = TransportChannelPool::make_channel(grpc_timeout, uri.clone()).await?;
            channels.push(channel);
        }
        Ok(Self {
            channels,
            init_at: Instant::now(),
        })
    }

    fn choose(&self) -> Channel {
        self.channels
            .choose(&mut rand::thread_rng())
            .expect("Pool size can not be zero")
            .clone()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error<E: std::error::Error> {
    #[error("Error in closure supplied to transport channel pool: {0}")]
    FromClosure(E),
    #[error("Tonic error: {0}")]
    Tonic(#[from] TonicError),
}

/// Holds a pool of channels established for a set of URIs.
/// Channel are shared by cloning them.
/// Make the `pool_size` larger to increase throughput.
pub struct TransportChannelPool {
    uri_to_pool: tokio::sync::RwLock<HashMap<Uri, ChannelsToUri>>,
    pool_size: NonZeroUsize,
    grpc_timeout: Duration,
}

impl TransportChannelPool {
    pub fn new(p2p_grpc_timeout: Duration, pool_size: NonZeroUsize) -> Self {
        Self {
            uri_to_pool: Default::default(),
            grpc_timeout: p2p_grpc_timeout,
            pool_size,
        }
    }

    pub async fn make_channel(grpc_timeout: Duration, uri: Uri) -> Result<Channel, TonicError> {
        let endpoint = Channel::builder(uri)
            .timeout(grpc_timeout)
            .connect_timeout(grpc_timeout)
            .keep_alive_while_idle(true);
        // `connect` is using the `Reconnect` network service internally to handle dropped connections
        endpoint.connect().await
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel<E: std::error::Error, O: Future<Output = Result<(), E>>>(
        &self,
        uri: &Uri,
        f: impl FnOnce(Channel) -> O,
    ) -> Result<(), Error<E>> {
        use std::collections::hash_map::Entry;
        let mut guard = self.uri_to_pool.write().await;
        match guard.entry(uri.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(
                    ChannelsToUri::init(uri.clone(), self.pool_size, self.grpc_timeout).await?,
                );
            }
            Entry::Occupied(_) => (),
        };
        let channels = guard.get(uri).expect("Unreachable");
        let result = f(channels.choose()).await;
        // Reconnect on failure to handle the case with domain name change.
        if result.is_err() && Instant::now().duration_since(channels.init_at) > CHANNEL_TTL {
            guard.insert(
                uri.clone(),
                ChannelsToUri::init(uri.clone(), self.pool_size, self.grpc_timeout).await?,
            );
        }
        result.map_err(Error::FromClosure)
    }
}
