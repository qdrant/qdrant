use std::alloc::Layout;
use std::borrow::Cow;

use common::fs::atomic_save;
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFs;
use common::universal_io::{MmapFs, UniversalReadFs};
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

    fn assert_base_vector(&self, point_id: PointOffsetType, level: usize, bytes: &[u8]) {
        if level == 0 {
            assert_eq!(bytes, self.base_vectors[point_id as usize]);
        } else {
            assert!(bytes.is_empty());
        }
    }

    fn assert_link_vector(&self, link: PointOffsetType, bytes: &[u8]) {
        assert_eq!(bytes, self.link_vectors[link as usize]);
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
            vectors.assert_base_vector(point_id, level, base_vector);
            iter.map(|(link, bytes)| {
                vectors.assert_link_vector(link, bytes);
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

    let cmp_links =
        GraphLinks::load_universal(&MmapFs, &links_file, format, GraphLinksResidency::Cold)
            .unwrap();
    check_links(links, &cmp_links, &vectors);
}

/// Test that [`GraphLinksFile`] returns the same links as the ones passed to
/// [`serialize_graph_links`].
#[rstest]
#[case::mmap(MmapFs)]
#[cfg_attr(target_os = "linux", case::io_uring(IoUringFs))]
fn test_links_file_links<F: UniversalReadFs>(
    #[case] fs: F,
    #[values(
        // GraphLinksFormat::Compressed
        None,
        // GraphLinksFormat::CompressedWithVectors
        Some((4, 16)), // f32, binary q
        Some((4, 1)), // f32, scalar q u8
        Some((1, 1)), // u8, scalar q u8
        // Fantasy layouts to catch alignment issues. (glibc min alignment is 16)
        Some((1, 64)), Some((64, 1)), Some((64, 64)))
    ]
    aligns: Option<(usize, usize)>,
) {
    let format = match aligns {
        None => GraphLinksFormat::Compressed,
        Some(_) => GraphLinksFormat::CompressedWithVectors,
    };
    let hnsw_m = HnswM::new2(8);
    let links = random_links(1000, 10, &hnsw_m);
    let vectors = aligns
        .map(|(base_align, link_align)| TestGraphLinksVectors::new(1000, base_align, link_align));

    let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
    let links_file = path.path().join("links.bin");
    atomic_save(&links_file, |writer| {
        serialize_graph_links(
            links.clone(),
            format.with_param_for_tests(vectors.as_ref()),
            hnsw_m,
            writer,
        )
    })
    .unwrap();

    let file = fs
        .open(
            &links_file,
            GraphLinks::open_options(GraphLinksResidency::Cold),
            Default::default(),
        )
        .unwrap();
    let view = GraphLinksFile::<F::File>::open(file, format).unwrap();
    let mut arena = stumpalo::Arena::new();
    let max_levels = links.iter().map(|levels| levels.len()).max().unwrap_or(0);
    for level in 0..max_levels {
        let level_m = hnsw_m.level_m(level);
        let point_ids = (0..links.len() as PointOffsetType)
            .filter(|&id| links[id as usize].len() > level)
            .collect::<Vec<_>>();
        for chunk in point_ids.chunks(17) {
            let check = |position: usize, links_iter: Vec<PointOffsetType>| {
                assert_eq!(
                    normalize_links(level_m, links_iter),
                    normalize_links(level_m, links[chunk[position] as usize][level].clone()),
                );
            };
            view.links(&mut arena, chunk, level, |position, iter| {
                check(position, iter.collect())
            })
            .unwrap();
            if let Some(vectors) = &vectors {
                view.links_with_vectors(
                    &mut arena,
                    chunk,
                    level,
                    |position, base_vector, iter, link_vectors| {
                        vectors.assert_base_vector(chunk[position], level, base_vector);
                        let links_iter = iter
                            .zip(link_vectors)
                            .map(|(link, bytes)| {
                                vectors.assert_link_vector(link, bytes);
                                link
                            })
                            .collect();
                        check(position, links_iter);
                        Ok(())
                    },
                )
                .unwrap();
            }
        }
    }
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
