use std::fs::OpenOptions;
use std::path::Path;

use common::types::PointOffsetType;
use memmap2::Mmap;

use super::graph_links::{
    get_level_offsets, get_links_slice, get_offsets_iter, get_reindex_slice, GraphLinks,
    GraphLinksConverter, GraphLinksFileHeader,
};
use crate::common::operation_error::OperationResult;
use crate::common::vector_utils::TrySetCapacityExact;

#[derive(Debug, Default)]
pub struct GraphLinksCompressed {
    // all flattened links of all levels
    links: Vec<PointOffsetType>,
    // all ranges in `links`. each range is `links[offsets[i]..offsets[i+1]]`
    // ranges are sorted by level
    offsets: Vec<u64>,
    // start offset of each level in `offsets`
    level_offsets: Vec<u64>,
    // for level 1 and above: reindex[point_id] = index of point_id in offsets
    reindex: Vec<PointOffsetType>,
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
        self.reindex.len()
    }

    fn links(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> impl Iterator<Item = PointOffsetType> {
        todo!()
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        todo!()
    }
}

impl GraphLinksCompressed {
    pub fn load_from_memory(data: &[u8]) -> OperationResult<Self> {
        let header = GraphLinksFileHeader::deserialize_bytes_from(data);

        let link_slice = get_links_slice(data, &header);
        // offsets.try_set_capacity_exact(header.get_offsets_range().len() / size_of::<u64>())?;
        get_offsets_iter(data, &header);

        let level_offsets_slice = get_level_offsets(data, &header);
        let reindex_slice = get_reindex_slice(data, &header);

        todo!()
    }
}
