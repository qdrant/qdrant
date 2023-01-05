use std::sync::Arc;
use std::{env, fmt, str, time};

use anyhow::{Context as _, Result};
use criterion::*;
use qdrant_client::prelude::*;
use qdrant_client::qdrant::vectors_config;
use rand::prelude::*;
use rand::rngs::StdRng;
use tokio::runtime::{self, Runtime};

criterion_main!(search_points_group);

criterion_group! {
    name = search_points_group;

    config = Criterion::default()
        .sample_size(1000)
        .warm_up_time(time::Duration::from_secs(10))
        .measurement_time(time::Duration::from_secs(60));

    targets = search_points
}

fn search_points(criterion: &mut Criterion) {
    let config = Config::parse();

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = client(&config, &runtime).unwrap();
    let vector_size = vector_size(&config, &runtime, &client).unwrap();

    let request = config.search_points(vector_size);
    let mut rng = config.rng();

    criterion.bench_function("search_points", move |bench| {
        bench.to_async(&runtime).iter_batched(
            || (client.clone(), randomize(&request, &mut rng)),
            |(client, request)| async move { client.search_points(&request).await.unwrap() },
            BatchSize::SmallInput,
        )
    });
}

#[derive(Clone, Debug)]
struct Config {
    uri: String,
    timeout: time::Duration,
    connection_timeout: time::Duration,
    api_key: Option<String>,
    collection_name: String,
    limit: u64,
    seed: u64,
}

impl Config {
    pub fn parse() -> Self {
        let uri = env_var("URI").unwrap_or_else(|| "http://127.0.0.1:6334".into());

        let timeout = time::Duration::from_secs_f32(parse_or(
            env_var("REQUEST_TIMEOUT").or_else(|| env_var("REQUEST")),
            10.,
        ));

        let connection_timeout =
            time::Duration::from_secs_f32(parse_or(env_var("CONNECTION_TIMEOUT"), 5.));

        let api_key = env_var("API_KEY");

        let collection_name = env_var("COLLECTION_NAME")
            .or_else(|| env_var("COLLECTION"))
            .unwrap_or_else(|| "benchmark".into());

        let limit = parse_or(env_var("LIMIT"), 10);
        let seed = parse_or(env_var("SEED"), 42);

        Self {
            uri,
            timeout,
            connection_timeout,
            api_key,
            collection_name,
            limit,
            seed,
        }
    }

    pub async fn client(&self) -> Result<QdrantClient> {
        let config = QdrantClientConfig {
            uri: self.uri.clone(),
            timeout: self.timeout,
            connect_timeout: self.connection_timeout,
            keep_alive_while_idle: false,
            api_key: self.api_key.clone(),
        };

        let client = QdrantClient::new(Some(config)).await?;

        Ok(client)
    }

    pub fn search_points(&self, vector_size: usize) -> SearchPoints {
        SearchPoints {
            collection_name: self.collection_name.clone(),
            vector: vec![0.; vector_size],
            limit: self.limit,
            ..Default::default()
        }
    }

    pub fn rng(&self) -> StdRng {
        StdRng::seed_from_u64(self.seed)
    }
}

fn env_var(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(var) => Some(var),
        Err(env::VarError::NotPresent) => None,
        Err(err) => panic!("failed to get `{}` env-var: {}", name, err),
    }
}

fn parse_or<T>(opt: Option<impl AsRef<str>>, default: T) -> T
where
    T: str::FromStr,
    T::Err: fmt::Debug,
{
    opt.as_ref()
        .map_or(default, |val| val.as_ref().parse().unwrap())
}

fn client(config: &Config, runtime: &Runtime) -> Result<Arc<QdrantClient>> {
    let client = runtime.block_on(config.client())?;
    Ok(Arc::new(client))
}

fn vector_size(config: &Config, runtime: &Runtime, client: &QdrantClient) -> Result<usize> {
    let response = runtime.block_on(client.collection_info(&config.collection_name))?;

    let vectors_config = response
        .result
        .context("`GetCollectionInfoResponse::result` is `None`")?
        .config
        .context("`CollectionInfo::config` is `None`")?
        .params
        .context("`CollectionConfig::params` is `None`")?
        .vectors_config
        .context("`CollectionParams::vectors_config` is `None`")?
        .config
        .context("`VectorsConfig::config` is `None`")?;

    let vector_params = match vectors_config {
        vectors_config::Config::Params(params) => params,
        _ => {
            return Err(anyhow::format_err!(
                "`search_points` only supports collections with a single vector per point"
            ));
        }
    };

    let vector_size = vector_params
        .size
        .try_into()
        .context("failed to convert `VectorParams::size` to `usize`")?;

    Ok(vector_size)
}

fn randomize(request: &SearchPoints, rng: &mut StdRng) -> SearchPoints {
    let mut request = request.clone();
    request.vector = (0..request.vector.len()).map(|_| rng.gen()).collect();
    request
}
