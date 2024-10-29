use std::fs::OpenOptions;
use std::ops::Range;
use std::path::Path;

use common::types::PointOffsetType;
use memmap2::Mmap;

use super::graph_links::{
    get_level_offsets, get_links_slice, get_reindex_slice, GraphLinks, GraphLinksConverter,
    GraphLinksFileHeader,
};
use super::links_compressor::{compress, DecompressIterator};
use crate::common::operation_error::OperationResult;
use crate::common::vector_utils::TrySetCapacityExact;

const ENTRY_BLOCK_SIZE: usize = 64;
const PADDING_SIZE: usize = 512;

#[derive(Debug, Default)]
pub struct GraphLinksCompressed {
    links: Vec<u8>,
    links_start: Vec<u8>,
    entry: Vec<u8>,
    entry_blocks: Vec<usize>,
    num_points: usize,
}

impl GraphLinks for GraphLinksCompressed {
    fn load_from_file(path: &Path) -> OperationResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;

        let mmap = unsafe { Mmap::map(&file)? };

        Self::load_from_memory(&mmap)
    }

    fn from_converter(converter: GraphLinksConverter) -> OperationResult<Self> {
        let mut data = vec![0; converter.data_size() as usize];
        converter.serialize_to(&mut data);
        drop(converter);

        Self::load_from_memory(&data)
    }

    fn num_points(&self) -> usize {
        self.num_points
    }

    fn links(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> impl Iterator<Item = PointOffsetType> {
        let entry_block = point_id as usize / ENTRY_BLOCK_SIZE;
        let entry_block_i = point_id as usize % ENTRY_BLOCK_SIZE;
        let entries = &self.entry[self.entry_blocks[entry_block]..];
        let level_start = DecompressIterator::new(entries.iter().cloned())
            .skip(entry_block_i)
            .next()
            .unwrap() as usize;

        let levels_data = &self.links_start[level_start..];
        let mut iter = DecompressIterator::new(levels_data.iter().cloned());
        let levels_count = iter.next().unwrap() as usize;
        if level >= levels_count {
            panic!("Level {} is out of bounds", level);
        }

        let mut iter = iter.skip(level);
        let links_start = iter.next().unwrap() as usize;
        let links_end = iter.next().unwrap() as usize;

        let links_data = &self.links[links_start..links_end];
        DecompressIterator::new(links_data.iter().cloned()).map(|l| l as PointOffsetType)
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        let entry_block = point_id as usize / ENTRY_BLOCK_SIZE;
        let entry_block_i = point_id as usize % ENTRY_BLOCK_SIZE;
        let entries = &self.entry[self.entry_blocks[entry_block]..];
        let level_start = DecompressIterator::new(entries.iter().cloned())
            .skip(entry_block_i)
            .next()
            .unwrap() as usize;

        let levels_data = &self.links_start[level_start..];
        DecompressIterator::new(levels_data.iter().cloned())
            .next()
            .unwrap() as usize
    }
}

impl GraphLinksCompressed {
    pub fn load_from_memory(data: &[u8]) -> OperationResult<Self> {
        let header = GraphLinksFileHeader::deserialize_bytes_from(data);
        let mmap = GraphLinksWrapper::load_from_memory(data, &header)?;
        let num_points = mmap.num_points();

        let mut links = vec![0u8; PADDING_SIZE];
        let mut links_start = vec![];
        let mut entry = vec![];
        let mut entry_blocks = vec![0];

        let mut e = vec![0];
        let mut l = vec![];
        let mut lnk = vec![];
        for point_id in 0..num_points {
            if point_id % ENTRY_BLOCK_SIZE == 0 && e.len() > 0 {
                compress(&e, &mut entry);
                entry_blocks.push(entry.len());
                e.clear();
            }

            e.push(entry.len() as u64);

            let levels_count = mmap.point_level(point_id as PointOffsetType) + 1;
            l.clear();
            l.push(levels_count as u64);

            l.push(links.len() as u64);
            for level in 0..levels_count {
                lnk.clear();
                lnk.extend(
                    mmap.links(point_id as PointOffsetType, level)
                        .iter()
                        .map(|x| *x as u64),
                );
                lnk.sort();

                compress(&lnk, &mut links);
                l.push(links.len() as u64);
            }

            compress(&l, &mut links_start);
        }
        if e.len() > 0 {
            compress(&e, &mut entry);
            entry_blocks.push(entry.len());
            e.clear();
        }

        log::info!(
            "Loaded Compressed: SIZE: {}, points: {}, levels: {}, links: {}, offsets: {}",
            links.len() + links_start.len() + entry.len() + entry_blocks.len() * size_of::<usize>(),
            links.len(),
            links_start.len(),
            entry.len(),
            entry_blocks.len(),
        );

        Ok(Self {
            links,
            links_start,
            entry,
            entry_blocks,
            num_points: mmap.num_points(),
        })
    }
}

struct GraphLinksWrapper<'a> {
    // all flattened links of all levels
    links: &'a [PointOffsetType],
    // all ranges in `links`. each range is `links[offsets[i]..offsets[i+1]]`
    // ranges are sorted by level
    offsets: &'a [u8],
    // start offset of each level in `offsets`
    level_offsets: &'a [u64],
    // for level 1 and above: reindex[point_id] = index of point_id in offsets
    reindex: &'a [PointOffsetType],
}

impl<'a> GraphLinksWrapper<'a> {
    pub fn load_from_memory(
        data: &'a [u8],
        header: &'a GraphLinksFileHeader,
    ) -> OperationResult<Self> {
        let link_slice = get_links_slice(data, &header);
        let level_offsets = get_level_offsets(data, &header);
        let reindex = get_reindex_slice(data, &header);
        let offsets_range = header.get_offsets_range();

        Ok(Self {
            links: link_slice,
            offsets: &data[offsets_range],
            level_offsets,
            reindex,
        })
    }

    fn num_points(&self) -> usize {
        self.reindex.len()
    }

    fn links(&self, point_id: PointOffsetType, level: usize) -> &'a [PointOffsetType] {
        if level == 0 {
            let links_range = self.get_links_range(point_id as usize);
            self.get_links(links_range)
        } else {
            let reindexed_point_id = self.reindex(point_id) as usize;
            let layer_offsets_start = self.get_level_offset(level);
            let links_range = self.get_links_range(layer_offsets_start + reindexed_point_id);
            self.get_links(links_range)
        }
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed_point_id = self.reindex(point_id) as usize;
        // level 0 is always present, start checking from level 1. Stop checking when level is incorrect
        for level in 1.. {
            if let Some(offsets_range) = self.get_level_offsets_range(level) {
                if offsets_range.start + reindexed_point_id >= offsets_range.end {
                    // incorrect level because point_id is out of range
                    return level - 1;
                }
            } else {
                // incorrect level because this level is larger that available levels
                return level - 1;
            }
        }
        unreachable!()
    }

    fn offsets_len(&self) -> usize {
        self.offsets.len() / size_of::<u64>()
    }

    fn levels_count(&self) -> usize {
        self.level_offsets.len()
    }

    fn get_links(&self, range: Range<usize>) -> &'a [PointOffsetType] {
        &self.links[range]
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let start_bytes = &self.offsets[idx * size_of::<u64>()..(idx + 1) * size_of::<u64>()];
        let start = u64::from_ne_bytes(start_bytes.try_into().unwrap());
        let end_bytes = &self.offsets[(idx + 1) * size_of::<u64>()..(idx + 2) * size_of::<u64>()];
        let end = u64::from_ne_bytes(end_bytes.try_into().unwrap());
        start as usize..end as usize
    }

    fn get_level_offset(&self, level: usize) -> usize {
        self.level_offsets[level] as usize
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        self.reindex[point_id as usize]
    }

    fn get_level_offsets_range(&self, level: usize) -> Option<Range<usize>> {
        if level < self.levels_count() {
            let layer_offsets_start = self.get_level_offset(level);
            let layer_offsets_end = if level + 1 < self.levels_count() {
                // `level` is not last, next level_offsets is end of range
                self.get_level_offset(level + 1)
            } else {
                // `level` is last, next `offsets.len()` is end of range
                self.offsets_len() - 1
            };
            Some(layer_offsets_start..layer_offsets_end)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::Rng;
    use tempfile::Builder;

    use super::*;

    fn to_vec<TGraphLinks: GraphLinks>(links: &TGraphLinks) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut result = Vec::new();
        let num_points = links.num_points();
        for i in 0..num_points {
            let mut layers = Vec::new();
            let num_levels = links.point_level(i as PointOffsetType) + 1;
            for level in 0..num_levels {
                let links = links.links(i as PointOffsetType, level).collect_vec();
                layers.push(links);
            }
            result.push(layers);
        }
        result
    }

    fn random_links(
        points_count: usize,
        max_levels_count: usize,
    ) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut rng = rand::thread_rng();
        (0..points_count)
            .map(|_| {
                let levels_count = rng.gen_range(1..max_levels_count);
                (0..levels_count)
                    .map(|_| {
                        let links_count = rng.gen_range(0..max_levels_count);
                        (0..links_count)
                            .map(|_| rng.gen_range(0..points_count) as PointOffsetType)
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    /// Test that random links can be saved by `GraphLinksConverter` and loaded correctly by a GraphLinks impl.
    fn test_save_load<A>(points_count: usize, max_levels_count: usize)
    where
        A: GraphLinks,
    {
        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count);
        {
            let mut links_converter = GraphLinksConverter::new(links.clone());
            links_converter.save_as(&links_file).unwrap();
        }
        let cmp_links = to_vec(&A::load_from_file(&links_file).unwrap());
        assert_eq!(links, cmp_links);
    }

    #[test]
    fn test_graph_links_construction() {
        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 4 levels with random nonexistent links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // fully random links
        let links = random_links(100, 10);
        let cmp_links = to_vec(
            &GraphLinksCompressed::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);
    }

    #[test]
    fn test_graph_links_mmap_ram_compatibility() {
        test_save_load::<GraphLinksCompressed>(1000, 10);
        test_save_load::<GraphLinksCompressed>(1000, 10);
    }
}
