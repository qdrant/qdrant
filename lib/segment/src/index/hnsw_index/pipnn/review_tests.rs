//! TODO(review): throwaway checks of the review findings, PiPNN against HNSW on the same data.
//!
//! ```text
//! cargo test --profile perf -p segment --lib pipnn::review_tests -- --nocapture --test-threads 1
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64};

use common::ambient;
use common::bitvec::BitVec;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

use super::{PipnnParams, build_links};
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers::{GraphLayers, GraphLayersBase as _, SearchAlgorithm};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_links::GraphLinksFormatParam;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::Distance;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::matrix_scorer::new_matrix_scorer;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum, VectorStorageRead as _};

const M: usize = 16;
const EF_CONSTRUCT: usize = 100;

type Adjacency = Vec<Vec<PointOffsetType>>;

fn storage(vectors: &[Vec<f32>], distance: Distance) -> VectorStorageEnum {
    let mut storage = new_volatile_dense_vector_storage(vectors[0].len(), distance);
    for (id, vector) in vectors.iter().enumerate() {
        let vector = distance.preprocess_vector::<VectorElementType>(vector.clone());
        storage
            .insert_vector(id as PointOffsetType, VectorRef::from(&vector))
            .unwrap();
    }
    storage
}

/// Everything on level 0, so level 0 alone has to connect the points.
fn builder(n: usize) -> GraphLayersBuilder {
    let mut builder = GraphLayersBuilder::new(n, HnswM::new2(M), EF_CONSTRUCT, 1, true);
    for id in 0..n {
        builder.set_levels(id as PointOffsetType, 0);
    }
    builder
}

/// The graph, its level 0 links, and its entry point.
fn finish(builder: GraphLayersBuilder, n: usize) -> (GraphLayers, Adjacency, PointOffsetType) {
    let entry = builder
        .get_entry_points()
        .get_entry_point(|_| true)
        .unwrap()
        .point_id;
    let graph = builder.into_graph_layers_ram(GraphLinksFormatParam::Plain);
    let adjacency = (0..n as PointOffsetType)
        .map(|id| {
            let mut links = Vec::new();
            graph.for_each_link(id, 0, |link| links.push(link));
            links
        })
        .collect();
    (graph, adjacency, entry)
}

fn pipnn(storage: &VectorStorageEnum, n: usize) -> GraphLayersBuilder {
    let deleted = BitVec::repeat(false, n);
    let scorer = new_matrix_scorer(storage, None).unwrap();
    let points: Vec<PointOffsetType> = (0..n as PointOffsetType).collect();
    let params = PipnnParams {
        seed: 42,
        ..PipnnParams::default()
    };
    let links = build_links(
        storage,
        None,
        &deleted,
        &scorer,
        &points,
        |_| 0,
        HnswM::new2(M),
        &params,
        &AtomicBool::new(false),
        &AtomicU64::new(0),
    )
    .unwrap()
    .unwrap();
    let builder = builder(n);
    for (&point, links) in std::iter::zip(&points, links) {
        builder.add_new_point(point, links);
    }
    builder
}

/// Sequential insertion, so the graph is reproducible.
fn hnsw(storage: &VectorStorageEnum, n: usize) -> GraphLayersBuilder {
    let deleted = BitVec::repeat(false, n);
    let builder = builder(n);
    for point in 0..n as PointOffsetType {
        let scorer =
            FilteredScorer::new_internal(point, storage, None::<&QuantizedVectors>, None, &deleted)
                .unwrap();
        builder.link_new_point(point, scorer);
    }
    builder
}

/// Points reachable from `start` over the directed links.
fn reachable(adjacency: &Adjacency, start: PointOffsetType) -> Vec<bool> {
    let mut seen = vec![false; adjacency.len()];
    let mut queue = VecDeque::from([start]);
    seen[start as usize] = true;
    while let Some(point) = queue.pop_front() {
        for &next in &adjacency[point as usize] {
            if !std::mem::replace(&mut seen[next as usize], true) {
                queue.push_back(next);
            }
        }
    }
    seen
}

fn in_degrees(adjacency: &Adjacency) -> Vec<usize> {
    let mut degrees = vec![0; adjacency.len()];
    for links in adjacency {
        for &link in links {
            degrees[link as usize] += 1;
        }
    }
    degrees
}

/// Of `group`, the members found by a search for `query` from the graph's entry point.
fn found(
    graph: &GraphLayers,
    storage: &VectorStorageEnum,
    query: &[f32],
    distance: Distance,
    group: &[PointOffsetType],
    ef: usize,
) -> usize {
    let deleted = BitVec::repeat(false, storage.total_vector_count());
    let query = distance.preprocess_vector::<VectorElementType>(query.to_vec());
    let mut scorer = FilteredScorer::new(
        query.into(),
        storage,
        None::<&QuantizedVectors>,
        None,
        &deleted,
    )
    .unwrap();
    let result = graph
        .search(
            group.len(),
            ef,
            SearchAlgorithm::Hnsw,
            &mut scorer,
            &graph.unfiltered_entry_point(),
            &AtomicBool::new(false),
        )
        .unwrap();
    result.iter().filter(|s| group.contains(&s.idx)).count()
}

fn random(rng: &mut StdRng, n: usize, dim: usize, center: f32) -> Vec<Vec<f32>> {
    (0..n)
        .map(|_| {
            (0..dim)
                .map(|_| center + rng.random_range(-1.0..1.0))
                .collect()
        })
        .collect()
}

/// Reachable points from the entry point and from point 0, for both builders.
fn report(name: &str, adjacency: &Adjacency, entry: PointOffsetType) {
    let n = adjacency.len();
    let from_entry = reachable(adjacency, entry).iter().filter(|&&r| r).count();
    let from_0 = reachable(adjacency, 0).iter().filter(|&&r| r).count();
    eprintln!("  {name}: reachable from entry {entry}: {from_entry}/{n}, from 0: {from_0}/{n}");
}

/// Finding 1, exactly as described: 10 identical vectors and nothing else.
#[test]
fn duplicates_only() {
    let _scope = ambient::test_guard();
    for distance in [Distance::Euclid, Distance::Cosine] {
        let vectors = vec![vec![0.5_f32, -1.0, 2.0, 0.25, 3.0, -0.5, 1.5, 1.0]; 10];
        let storage = storage(&vectors, distance);
        eprintln!("10 identical vectors, {distance:?}");
        for (name, builder) in [("PiPNN", pipnn(&storage, 10)), ("HNSW", hnsw(&storage, 10))] {
            let (graph, adjacency, entry) = finish(builder, 10);
            eprintln!("  {name} links: {adjacency:?}");
            report(name, &adjacency, entry);
            let group: Vec<_> = (0..10).collect();
            let hits = found(&graph, &storage, &vectors[0], distance, &group, 1000);
            eprintln!("  {name}: search ef 1000 finds {hits}/10 copies");
        }
    }
}

/// Finding 1 in context: 2000 random points and 20 copies of one of them.
#[test]
fn duplicates_among_random_points() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(7);
    let dim = 32;
    let mut vectors = random(&mut rng, 2000, dim, 0.0);
    let copies = 20;
    for _ in 0..copies {
        vectors.push(vectors[1234].clone());
    }
    let n = vectors.len();
    let group: Vec<PointOffsetType> = std::iter::once(1234)
        .chain(2000..n as PointOffsetType)
        .collect();
    for distance in [Distance::Euclid, Distance::Cosine] {
        let storage = storage(&vectors, distance);
        eprintln!("2000 random points + {copies} copies of point 1234 (dim {dim}), {distance:?}");
        for (name, builder) in [("PiPNN", pipnn(&storage, n)), ("HNSW", hnsw(&storage, n))] {
            let (graph, adjacency, entry) = finish(builder, n);
            let degrees = in_degrees(&adjacency);
            let seen = reachable(&adjacency, entry);
            let reached = group.iter().filter(|&&p| seen[p as usize]).count();
            let without_in_links = group.iter().filter(|&&p| degrees[p as usize] == 0).count();
            let others = (0..2000).filter(|&p| !seen[p]).count();
            eprintln!(
                "  {name}: group of {}: reachable {reached}, without in-links {without_in_links}, \
                 in-degrees {:?}; other points unreachable: {others}",
                group.len(),
                group
                    .iter()
                    .map(|&p| degrees[p as usize])
                    .collect::<Vec<_>>(),
            );
            let hits = found(&graph, &storage, &vectors[1234], distance, &group, 1000);
            eprintln!(
                "  {name}: search ef 1000 finds {hits}/{} copies",
                group.len()
            );
        }
    }
}

/// Finding 2, exactly as described: two far apart clusters of 65 points, one leaf.
#[test]
fn two_clusters_in_one_leaf() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(3);
    let mut vectors = random(&mut rng, 65, 16, 0.0);
    vectors.extend(random(&mut rng, 65, 16, 100.0));
    let n = vectors.len();
    let storage = storage(&vectors, Distance::Euclid);
    eprintln!("2 clusters x 65 points, dim 16, Euclid, one leaf");
    for (name, builder) in [("PiPNN", pipnn(&storage, n)), ("HNSW", hnsw(&storage, n))] {
        let (_, adjacency, entry) = finish(builder, n);
        report(name, &adjacency, entry);
        let cross = adjacency
            .iter()
            .enumerate()
            .flat_map(|(p, links)| links.iter().map(move |&q| (p < 65) != ((q as usize) < 65)))
            .filter(|&c| c)
            .count();
        eprintln!("  {name}: links between the clusters: {cross}");
    }
}

/// Finding 2 beyond one leaf: a small far cluster next to a large one, several leaves.
#[test]
fn small_far_cluster_across_leaves() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(5);
    for (small, offset) in [(30, 20.0), (30, 3.0), (200, 20.0)] {
        let mut vectors = random(&mut rng, 5000, 16, 0.0);
        vectors.extend(random(&mut rng, small, 16, offset));
        let n = vectors.len();
        let storage = storage(&vectors, Distance::Euclid);
        eprintln!("5000 points around 0 + {small} around {offset}, dim 16, Euclid");
        for (name, builder) in [("PiPNN", pipnn(&storage, n)), ("HNSW", hnsw(&storage, n))] {
            let (_, adjacency, entry) = finish(builder, n);
            let seen = reachable(&adjacency, entry);
            let small_reached = (5000..n).filter(|&p| seen[p]).count();
            let large_reached = (0..5000).filter(|&p| seen[p]).count();
            eprintln!(
                "  {name}: from entry {entry}: large {large_reached}/5000, small {small_reached}/{small}",
            );
        }
    }
}
