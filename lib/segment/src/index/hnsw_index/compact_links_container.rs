use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

#[derive(Deserialize, Serialize, Debug)]
pub struct CompactLinksContainer {
    links: Vec<PointOffsetType>,
    offsets: Vec<usize>,
}

impl CompactLinksContainer {
    pub fn new_empty(num_vectors: usize) -> Self {
        CompactLinksContainer {
            links: vec![0],
            offsets: vec![0; num_vectors + 1],
        }
    }

    pub fn build(link_container: &Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        let total_size = link_container
            .iter()
            .map(|v| v.get(0).map(|l| l.len()).unwrap_or(0))
            .sum();
        let mut links = Vec::with_capacity(total_size);
        let mut offsets = Vec::with_capacity(link_container.len() + 1);
        offsets.push(0);
        for layers in link_container {
            if let Some(layer_links) = layers.get(0) {
                links.extend_from_slice(layer_links);
            }
            offsets.push(links.len());
        }
        CompactLinksContainer { links, offsets }
    }

    pub fn get_links(&self, point_id: usize) -> &[PointOffsetType] {
        let start = self.offsets[point_id];
        let end = self.offsets[point_id + 1];
        &self.links[start..end]
    }

    #[cfg(test)]
    pub fn get_total_links(&self) -> usize {
        self.links.len()
    }
}
