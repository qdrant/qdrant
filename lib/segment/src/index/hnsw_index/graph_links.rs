use std::alloc::Layout;
use std::path::Path;
use std::sync::Arc;

use common::types::PointOffsetType;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting, Madviseable};
use memory::mmap_ops::open_read_mmap;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

mod header;
mod serializer;
mod view;

pub use serializer::GraphLinksSerializer;
pub use view::LinksIterator;
use view::{CompressionInfo, GraphLinksView, LinksWithVectorsIterator};

/*
Links data for whole graph layers.

                                    sorted
                     points:        points:
points to lvl        012345         142350
     0 -> 0
     1 -> 4    lvl4:  7       lvl4: 7
     2 -> 2    lvl3:  Z  Y    lvl3: ZY
     3 -> 2    lvl2:  abcd    lvl2: adbc
     4 -> 3    lvl1:  ABCDE   lvl1: ADBCE
     5 -> 1    lvl0: 123456   lvl0: 123456  <- lvl 0 is not sorted


lvl offset:        6       11     15     17
                   │       │      │      │
                   │       │      │      │
                   ▼       ▼      ▼      ▼
indexes:  012345   6789A   BCDE   FG     H

flatten:  123456   ADBCE   adbc   ZY     7
                   ▲ ▲ ▲   ▲ ▲    ▲      ▲
                   │ │ │   │ │    │      │
                   │ │ │   │ │    │      │
                   │ │ │   │ │    │      │
reindex:           142350  142350 142350 142350  (same for each level)


for lvl > 0:
links offset = level_offsets[level] + offsets[reindex[point_id]]
*/

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GraphLinksFormat {
    Plain,
    Compressed,
    CompressedWithVectors,
}

/// Similar to [`GraphLinksFormat`], won't let you use `CompressedWithVectors`
/// without providing the vectors.
#[derive(Clone, Copy)]
pub enum GraphLinksFormatParam<'a> {
    Plain,
    Compressed,
    CompressedWithVectors(&'a dyn GraphLinksVectors),
}

pub trait GraphLinksVectors {
    fn get_vector(&self, point_id: PointOffsetType) -> OperationResult<&[u8]>;

    fn vector_layout(&self) -> OperationResult<Layout>;
}

impl GraphLinksVectors for QuantizedVectors {
    fn get_vector(&self, point_id: PointOffsetType) -> OperationResult<&[u8]> {
        Ok(self.get_quantized_vector(point_id))
    }

    fn vector_layout(&self) -> OperationResult<Layout> {
        self.get_quantized_vector_layout()
    }
}

#[cfg(test)]
impl GraphLinksVectors for Vec<Vec<u8>> {
    fn get_vector(&self, point_id: PointOffsetType) -> OperationResult<&[u8]> {
        Ok(&self[point_id as usize])
    }

    fn vector_layout(&self) -> OperationResult<Layout> {
        const ALIGNMENT: usize = 8; // Some hard-coded alignment for test purposes.
        Ok(
            Layout::from_size_align(self.first().map_or(ALIGNMENT, |v| v.len()), ALIGNMENT)
                .unwrap(),
        )
    }
}

impl GraphLinksFormat {
    /// Create the corresponding [`GraphLinksFormatParam`].
    ///
    /// # Panics
    ///
    /// Panics if `CompressedWithVectors` is selected, but `vectors` is `None`.
    #[cfg(test)]
    pub fn with_param_for_tests<'a, Q: GraphLinksVectors>(
        &self,
        vectors: Option<&'a Q>,
    ) -> GraphLinksFormatParam<'a> {
        match self {
            GraphLinksFormat::Plain => GraphLinksFormatParam::Plain,
            GraphLinksFormat::Compressed => GraphLinksFormatParam::Compressed,
            GraphLinksFormat::CompressedWithVectors => match vectors {
                Some(v) => GraphLinksFormatParam::CompressedWithVectors(v),
                None => panic!(),
            },
        }
    }

    /// Create the corresponding [`GraphLinksFormatParam`].
    ///
    /// When vectors are not available, `CompressedWithVectors` is downgraded to
    /// `Compressed`.
    pub fn with_param<'a, V: GraphLinksVectors>(
        &self,
        vectors: Option<&'a V>,
    ) -> GraphLinksFormatParam<'a> {
        match self {
            GraphLinksFormat::Plain => GraphLinksFormatParam::Plain,
            GraphLinksFormat::Compressed => GraphLinksFormatParam::Compressed,
            GraphLinksFormat::CompressedWithVectors => match vectors {
                Some(v) => GraphLinksFormatParam::CompressedWithVectors(v),
                None => GraphLinksFormatParam::Compressed,
            },
        }
    }

    pub fn is_with_vectors(&self) -> bool {
        match self {
            GraphLinksFormat::Plain | GraphLinksFormat::Compressed => false,
            GraphLinksFormat::CompressedWithVectors => true,
        }
    }
}

impl GraphLinksFormatParam<'_> {
    pub fn as_format(&self) -> GraphLinksFormat {
        match self {
            GraphLinksFormatParam::Plain => GraphLinksFormat::Plain,
            GraphLinksFormatParam::Compressed => GraphLinksFormat::Compressed,
            GraphLinksFormatParam::CompressedWithVectors(_) => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }
}

self_cell::self_cell! {
    pub struct GraphLinks {
        owner: GraphLinksEnum,
        #[covariant]
        dependent: GraphLinksView,
    }

    impl {Debug}
}

#[derive(Debug)]
enum GraphLinksEnum {
    Ram(Vec<u8>),
    Mmap(Arc<Mmap>),
}

impl GraphLinksEnum {
    fn load_view(&self, format: GraphLinksFormat) -> OperationResult<GraphLinksView<'_>> {
        let data = match self {
            GraphLinksEnum::Ram(data) => data.as_slice(),
            GraphLinksEnum::Mmap(mmap) => &mmap[..],
        };
        GraphLinksView::load(data, format)
    }
}

impl GraphLinks {
    pub fn load_from_file(
        path: &Path,
        on_disk: bool,
        format: GraphLinksFormat,
    ) -> OperationResult<Self> {
        let populate = !on_disk;
        let mmap = open_read_mmap(path, AdviceSetting::Advice(Advice::Random), populate)?;
        Self::try_new(GraphLinksEnum::Mmap(Arc::new(mmap)), |x| {
            x.load_view(format)
        })
    }

    fn view(&self) -> &GraphLinksView<'_> {
        self.borrow_dependent()
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self.view().compression {
            CompressionInfo::Uncompressed { .. } => GraphLinksFormat::Plain,
            CompressionInfo::Compressed { .. } => GraphLinksFormat::Compressed,
            CompressionInfo::CompressedWithVectors { .. } => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }

    pub fn on_disk(&self) -> bool {
        matches!(self.borrow_owner(), GraphLinksEnum::Ram(_))
    }

    pub fn num_points(&self) -> usize {
        self.view().reindex.len()
    }

    pub fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.links(point_id, level).for_each(f);
    }

    #[inline]
    pub fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        self.view().links(point_id, level)
    }

    #[inline]
    pub fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> LinksWithVectorsIterator<'_> {
        let (links, vectors) = self.view().links_with_vectors(point_id, level);
        links.zip(vectors)
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }

    /// Convert the graph links to a vector of edges, suitable for passing into
    /// [`GraphLinksSerializer::new`] or using in tests.
    pub fn to_edges(&self) -> Vec<Vec<Vec<PointOffsetType>>> {
        self.to_edges_impl(|point_id, level| self.links(point_id, level).collect())
    }

    /// Convert the graph links to a vector of edges, generic over the container type.
    pub fn to_edges_impl<Container>(
        &self,
        mut f: impl FnMut(PointOffsetType, usize) -> Container,
    ) -> Vec<Vec<Container>> {
        let mut edges = Vec::with_capacity(self.num_points());
        for point_id in 0..self.num_points() {
            let num_levels = self.point_level(point_id as PointOffsetType) + 1;
            let mut levels = Vec::with_capacity(num_levels);
            levels.extend((0..num_levels).map(|level| f(point_id as PointOffsetType, level)));
            edges.push(levels);
        }
        edges
    }

    /// Populate the disk cache with data, if applicable.
    /// This is a blocking operation.
    pub fn populate(&self) -> OperationResult<()> {
        match self.borrow_owner() {
            GraphLinksEnum::Mmap(mmap) => mmap.populate(),
            GraphLinksEnum::Ram(_) => {}
        };
        Ok(())
    }
}

/// Sort the first `m` values in `links` and return them. Used to compare stored
/// links where the order of the first `m` links is not preserved.
#[cfg(test)]
pub(super) fn normalize_links(m: usize, mut links: Vec<PointOffsetType>) -> Vec<PointOffsetType> {
    let first = links.len().min(m);
    links[..first].sort_unstable();
    links
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::index::hnsw_index::HnswM;

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

    fn random_vectors(points_count: usize, bytes_size: usize) -> Vec<Vec<u8>> {
        let mut rng = rand::rng();
        (0..points_count)
            .map(|_| (0..bytes_size).map(|_| rng.random()).collect())
            .collect()
    }

    fn check_links(
        mut left: Vec<Vec<Vec<PointOffsetType>>>,
        right: &GraphLinks,
        vectors: &Option<Vec<Vec<u8>>>,
    ) {
        let mut right_links = right.to_edges_impl(|point_id, level| {
            if let Some(vectors) = vectors {
                right
                    .links_with_vectors(point_id, level)
                    .inspect(|&(link, bytes)| assert_eq!(bytes, vectors[link as usize]))
                    .map(|(link, _bytes)| link)
                    .collect()
            } else {
                right.links(point_id, level).collect()
            }
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

    /// Test that random links can be saved by [`GraphLinksSerializer`] and
    /// loaded correctly by a [`GraphLinks`] impl.
    #[rstest]
    #[case(GraphLinksFormat::Plain, true)]
    #[case(GraphLinksFormat::Plain, false)]
    #[case(GraphLinksFormat::Compressed, true)]
    #[case(GraphLinksFormat::Compressed, false)]
    #[case(GraphLinksFormat::CompressedWithVectors, true)]
    #[case(GraphLinksFormat::CompressedWithVectors, false)]
    fn test_save_load(#[case] format: GraphLinksFormat, #[case] on_disk: bool) {
        let points_count = 1000;
        let max_levels_count = 10;
        let hnsw_m = HnswM::new2(8);

        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count, &hnsw_m);

        let vectors = format
            .is_with_vectors()
            .then(|| random_vectors(points_count, 8));

        GraphLinksSerializer::new(
            links.clone(),
            format.with_param_for_tests(vectors.as_ref()),
            hnsw_m,
        )
        .unwrap()
        .save_as(&links_file)
        .unwrap();
        let cmp_links = GraphLinks::load_from_file(&links_file, on_disk, format).unwrap();
        check_links(links, &cmp_links, &vectors);
    }

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_graph_links_construction(#[case] format: GraphLinksFormat) {
        let hnsw_m = HnswM::new2(8);

        let vectors = format.is_with_vectors().then(|| random_vectors(100, 16));

        let check = |links: Vec<Vec<Vec<PointOffsetType>>>| {
            let cmp_links = GraphLinksSerializer::new(
                links.clone(),
                format.with_param_for_tests(vectors.as_ref()),
                hnsw_m,
            )
            .unwrap()
            .to_graph_links_ram();
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
}
