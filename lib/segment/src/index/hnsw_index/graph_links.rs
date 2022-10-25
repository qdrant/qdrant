use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct LayerData {
    pub links: Vec<PointOffsetType>,
    pub offsets: Vec<usize>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GraphLinks {
    layers: Vec<LayerData>,
    reindex: Vec<PointOffsetType>,
}

impl GraphLinks {
    pub fn get_memsize(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .layers
                .iter()
                .map(|layer| {
                    std::mem::size_of::<LayerData>()
                        + layer.links.len() * std::mem::size_of::<PointOffsetType>()
                        + layer.offsets.len() * std::mem::size_of::<usize>()
                })
                .sum::<usize>()
            + self.reindex.len() * std::mem::size_of::<PointOffsetType>()
    }

    pub fn from_vec(edges: &Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        let mut reindex = (0..edges.len() as PointOffsetType).collect::<Vec<PointOffsetType>>();
        reindex.sort_unstable_by_key(|&i| -(edges[i as usize].len() as i32));

        let mut back_index = vec![0; edges.len()];
        for i in 0..edges.len() {
            back_index[reindex[i] as usize] = i as PointOffsetType;
        }

        let max_layers = edges.iter().map(|e| e.len()).max().unwrap_or(0);
        let mut layers = vec![
            LayerData {
                links: Vec::new(),
                offsets: vec![0],
            };
            max_layers
        ];

        for i in 0..edges.len() {
            // layer 0
            let layer_data = &mut layers[0];
            if let Some(links) = edges[i].get(0) {
                layer_data.links.extend_from_slice(links);
            }
            layer_data.offsets.push(layer_data.links.len());

            // other layers
            let i = reindex[i] as usize;
            for (layer, links) in edges[i].iter().enumerate() {
                if layer != 0 {
                    let layer_data = &mut layers[layer];
                    layer_data.links.extend_from_slice(&links);
                    layer_data.offsets.push(layer_data.links.len());
                }
            }
        }

        GraphLinks {
            layers,
            reindex: back_index,
        }
    }

    pub fn to_vec(&self) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut result = Vec::new();
        let num_points = self.num_points();
        for i in 0..num_points {
            let mut layers = Vec::new();
            let num_levels = self.point_level(i as PointOffsetType) + 1;
            for level in 0..num_levels {
                let links = self.links(i as PointOffsetType, level).to_vec();
                layers.push(links);
            }
            result.push(layers);
        }
        result
    }

    pub fn links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        let layer_data = &self.layers[level];
        if level == 0 {
            let start = layer_data.offsets[point_id as usize];
            let end = layer_data.offsets[point_id as usize + 1];
            &layer_data.links[start..end]
        } else {
            let reindexed = self.reindex[point_id as usize] as usize;
            let start = layer_data.offsets[reindexed];
            let end = layer_data.offsets[reindexed + 1];
            &layer_data.links[start..end]
        }
    }

    pub fn num_points(&self) -> usize {
        self.layers[0].offsets.len() - 1
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed = self.reindex[point_id as usize];
        for (layer, data) in self.layers.iter().enumerate() {
            if layer == 0 {
                continue;
            }
            if reindexed as usize + 1 >= data.offsets.len() {
                return layer - 1;
            }
        }
        return self.layers.len() - 1;
    }
}
