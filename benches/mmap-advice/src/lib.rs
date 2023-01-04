use std::time;

use qdrant_client::prelude::*;
use rand::prelude::*;
use rand::rngs::StdRng;

pub async fn client(uri: String) -> anyhow::Result<QdrantClient> {
    let config = QdrantClientConfig {
        uri,
        timeout: time::Duration::from_secs(20),
        connect_timeout: time::Duration::from_secs(10),
        keep_alive_while_idle: false,
        api_key: None,
    };

    let client = QdrantClient::new(Some(config)).await?;

    Ok(client)
}

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub mod search_points {
    use super::*;

    pub fn request(collection: String, dim: usize, limit: u64) -> SearchPoints {
        SearchPoints {
            collection_name: collection,
            vector: vec![0.; dim],
            limit,
            ..Default::default()
        }
    }

    pub fn randomize(request: &mut SearchPoints, rng: &mut StdRng) {
        request.vector = (0..request.vector.len()).map(|_| rng.gen()).collect();
    }

    pub async fn send(client: &QdrantClient, request: &SearchPoints) -> anyhow::Result<()> {
        let _ = client.search_points(request).await?;
        Ok(())
    }
}
