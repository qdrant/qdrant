use std::sync::Arc;
use std::{env, time};

use criterion::*;
use mmap_advice::*;

criterion_main!(benches);

criterion_group! {
    name = benches;

    config = Criterion::default()
        .sample_size(1000)
        .warm_up_time(time::Duration::from_secs(10))
        .measurement_time(time::Duration::from_secs(60));

    targets = search_points
}

fn search_points(crit: &mut Criterion) {
    let uri = var("QDRANT_URI").unwrap_or_else(|| "http://127.0.0.1:6334".into());
    let collection = var("QDRANT_URI").unwrap_or_else(|| "benchmark".into());
    let dim = var("QDRANT_DIM").map_or(128, |dim| dim.parse().unwrap());
    let limit = var("QDRANT_LIMIT").map_or(10, |limit| limit.parse().unwrap());
    let seed = var("QDRANT_SEED").map_or(0, |seed| seed.parse().unwrap());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = Arc::new(runtime.block_on(client(uri)).unwrap());

    let request = search_points::request(collection, dim, limit);
    let mut rng = rng(seed);

    crit.bench_function("search_points", move |bench| {
        bench.to_async(&runtime).iter_batched(
        || {
            let client = client.clone();
            let mut request = request.clone();
            search_points::randomize(&mut request, &mut rng);
            (client, request)
        },
        |(client, request)| async move { search_points::send(&client, &request).await.unwrap() },
        BatchSize::SmallInput,
    )
    });
}

fn var(var: &str) -> Option<String> {
    match env::var(var) {
        Ok(var) => Some(var),
        Err(env::VarError::NotPresent) => None,
        Err(err) => panic!("failed to get env-var `{}`: {}", var, err),
    }
}
