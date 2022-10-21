use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;


#[derive(Deserialize, Serialize, Debug)]
pub struct CompactLinksContainer {
    links: Vec<PointOffsetType>,
    offsets: Vec<usize>,
}

impl CompactLinksContainer {
    pub fn build(link_container: Vec<Vec<PointOffsetType>>) -> Self {
        let total_size = link_container.iter().map(|v| v.len()).sum();
        let mut links = Vec::with_capacity(total_size);
        let mut offsets = Vec::with_capacity(link_container.len() + 1);
        offsets.push(0);
        for layer_links in link_container {
            links.extend(layer_links);
            offsets.push(links.len());
        }
        CompactLinksContainer { links, offsets }
    }

    pub fn get_links(&self, point_id: usize) -> &[PointOffsetType] {
        let start = self.offsets[point_id];
        let end = self.offsets[point_id + 1];
        &self.links[start..end]
    }
}