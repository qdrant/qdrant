use std::alloc::Layout;
use std::borrow::Cow;

use common::fs::atomic_save;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use rand::RngExt;
use rstest::rstest;
use tempfile::Builder;

use super::*;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswM;

struct TestGraphLinksVectors {
    base_vectors: Vec<Vec<u8>>,
    link_vectors: Vec<Vec<u8>>,
    vectors_layout: GraphLinksVectorsLayout,
}

impl TestGraphLinksVectors {
    fn new(count: usize, base_align: usize, link_align: usize) -> Self {
        let mut rng = rand::rng();
        let base_len = base_align * 7;
        let link_len = link_align * 5;
        Self {
            base_vectors: (0..count)
                .map(|_| (0..base_len).map(|_| rng.random()).collect())
                .collect(),
            link_vectors: (0..count)
                .map(|_| (0..link_len).map(|_| rng.random()).collect())
                .collect(),
            vectors_layout: GraphLinksVectorsLayout {
                base: Layout::from_size_align(base_len, base_align).unwrap(),
                link: Layout::from_size_align(link_len, link_align).unwrap(),
            },
        }
    }
}

impl GraphLinksVectors for TestGraphLinksVectors {
    fn for_base_vector(
        &self,
        point_id: PointOffsetType,
        f: &mut dyn FnMut(&[u8]) -> OperationResult<()>,
    ) -> OperationResult<()> {
        f(&self.base_vectors[point_id as usize])
    }

    fn get_link_vector(&self, point_id: PointOffsetType) -> OperationResult<Cow<'_, [u8]>> {
        Ok(Cow::Borrowed(&self.link_vectors[point_id as usize]))
    }

    fn vectors_layout(&self) -> GraphLinksVectorsLayout {
        self.vectors_layout
    }
}

fn random_links(
    points_count: usize,
    max_levels_count: usize,
    hnsw_m: &HnswM,
) -> Vec<Vec<Vec<PointOffsetType>>> {
    let mut rng = rand::rng();
    (0..points_count)
        .map(|_| {
            let levels_count = rng.random_range(1..max_levels_count);
            (0..levels_count)
                .map(|level| {
                    let mut max_links_count = hnsw_m.level_m(level);
                    max_links_count *= 2; // Simulate additional payload links.
                    let links_count = rng.random_range(0..max_links_count);
                    (0..links_count)
                        .map(|_| rng.random_range(0..points_count) as PointOffsetType)
                        .collect()
                })
                .collect()
        })
        .collect()
}

fn check_links(
    mut left: Vec<Vec<Vec<PointOffsetType>>>,
    right: &GraphLinks,
    vectors: &Option<TestGraphLinksVectors>,
) {
    let mut right_links = right.to_edges_impl(|point_id, level| {
        let links: Vec<_> = if let Some(vectors) = vectors {
            let (base_vector, iter) = right.links_with_vectors(point_id, level);
            if level == 0 {
                vectors
                    .for_base_vector(point_id, &mut |bytes| {
                        assert_eq!(base_vector, bytes);
                        Ok(())
                    })
                    .unwrap();
            } else {
                assert!(base_vector.is_empty());
            }
            iter.map(|(link, bytes)| {
                assert_eq!(bytes, vectors.get_link_vector(link).unwrap().as_ref());
                link
            })
            .collect()
        } else {
            right.links(point_id, level).collect()
        };
        assert_eq!(links.is_empty(), right.links_empty(point_id, level));
        links
    });
    for links in [&mut left, &mut right_links].iter_mut() {
        links.iter_mut().for_each(|levels| {
            levels
                .iter_mut()
                .enumerate()
                .for_each(|(level_idx, links)| {
                    *links = normalize_links(
                        right.view().sorted_count(level_idx),
                        std::mem::take(links),
                    );
                })
        });
    }
    assert_eq!(left, right_links);
}

/// Test that random links can be saved by [`serialize_graph_links`] and
/// loaded correctly by a [`GraphLinks`] impl.
#[rstest]
#[case::plain(GraphLinksFormat::Plain, 8, 8)]
#[case::compressed(GraphLinksFormat::Compressed, 8, 8)]
#[case::comp_vec_1_16(GraphLinksFormat::CompressedWithVectors, 1, 16)]
#[case::comp_vec_4_1(GraphLinksFormat::CompressedWithVectors, 4, 1)]
#[case::comp_vec_4_16(GraphLinksFormat::CompressedWithVectors, 4, 16)]
fn test_save_load(
    #[case] format: GraphLinksFormat,
    #[case] base_align: usize,
    #[case] link_align: usize,
) {
    let points_count = 1000;
    let max_levels_count = 10;
    let hnsw_m = HnswM::new2(8);

    let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
    let links_file = path.path().join("links.bin");
    let links = random_links(points_count, max_levels_count, &hnsw_m);

    let vectors = format
        .is_with_vectors()
        .then(|| TestGraphLinksVectors::new(points_count, base_align, link_align));

    let format_param = format.with_param_for_tests(vectors.as_ref());
    atomic_save(&links_file, |writer| {
        serialize_graph_links(links.clone(), format_param, hnsw_m, writer)
    })
    .unwrap();

    let cmp_links = GraphLinks::load_universal(&MmapFs, &links_file, format, Populate::No).unwrap();
    check_links(links, &cmp_links, &vectors);
}

#[rstest]
#[case::uncompressed(GraphLinksFormat::Plain)]
#[case::compressed(GraphLinksFormat::Compressed)]
#[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
fn test_graph_links_construction(#[case] format: GraphLinksFormat) {
    let hnsw_m = HnswM::new2(8);

    let vectors = format
        .is_with_vectors()
        .then(|| TestGraphLinksVectors::new(100, 8, 8));

    let check = |links: Vec<Vec<Vec<PointOffsetType>>>| {
        let format_param = format.with_param_for_tests(vectors.as_ref());
        let cmp_links = GraphLinks::new_from_edges(links.clone(), format_param, hnsw_m).unwrap();
        check_links(links, &cmp_links, &vectors);
    };

    // no points
    check(vec![]);

    // 2 points without any links
    check(vec![vec![vec![]], vec![vec![]]]);

    // one link at level 0
    check(vec![vec![vec![1]], vec![vec![0]]]);

    // 3 levels with no links at second level
    check(vec![
        vec![vec![1, 2]],
        vec![vec![0, 2], vec![], vec![2]],
        vec![vec![0, 1], vec![], vec![1]],
    ]);

    // 3 levels with no links at last level
    check(vec![
        vec![vec![1, 2], vec![2], vec![]],
        vec![vec![0, 2], vec![1], vec![]],
        vec![vec![0, 1]],
    ]);

    // 4 levels with random nonexistent links
    check(vec![
        vec![vec![1, 2, 5, 6]],
        vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
        vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
        vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
        vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
    ]);

    // fully random links
    check(random_links(100, 10, &hnsw_m));
}
