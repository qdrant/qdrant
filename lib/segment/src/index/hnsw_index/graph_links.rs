use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

// Links data for whole layer.
// All links are flattened into one array.
// Range of links in this array for point with index `i` is `offsets[i]..offsets[(i + 1)]`.
#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct LayerData {
    pub links: Vec<PointOffsetType>,
    pub offsets: Vec<usize>,
}

// Links data for whole graph.
#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GraphLinks {
    // Links data for each layer.
    layers: Vec<LayerData>,

    // Reindexing for all layers except layer 0.
    // This map reindexes from point index `i` to index in `LayerData.offsets`
    // Reindexing sorts points by their max layer.
    // That's why we can use one reindex for all layers.
    reindex: Vec<PointOffsetType>,
}

impl GraphLinks {
    // Convert from graph layers builder links
    // `Vec<Vec<Vec<_>>>` means:
    // vector of points -> vector of layers for specific point -> vector of links for specific point and layer
    pub fn from_vec(edges: &Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        // create map from offsets to point id. sort by max layer and use this map to build `GraphLinks::reindex`
        let mut back_index = (0..edges.len() as PointOffsetType).collect::<Vec<PointOffsetType>>();
        back_index.sort_unstable_by_key(|&i| -(edges[i as usize].len() as i32));

        // reindex is map from point id to index in `LayerData.offsets`
        let mut reindex = vec![0; edges.len()];
        for i in 0..edges.len() {
            reindex[back_index[i] as usize] = i as PointOffsetType;
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
            // layer 0 doesn't use reindex
            let layer_data = &mut layers[0];
            if let Some(links) = edges[i].get(0) {
                layer_data.links.extend_from_slice(links);
            }
            layer_data.offsets.push(layer_data.links.len());

            // other layers use reindex
            let i = back_index[i] as usize;
            for (layer, links) in edges[i].iter().enumerate() {
                if layer != 0 {
                    let layer_data = &mut layers[layer];
                    layer_data.links.extend_from_slice(links);
                    layer_data.offsets.push(layer_data.links.len());
                }
            }
        }

        layers.iter_mut().for_each(|layer| {
            layer.links.shrink_to_fit();
            layer.offsets.shrink_to_fit();
        });
        layers.shrink_to_fit();
        back_index.shrink_to_fit();

        GraphLinks { layers, reindex }
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
        self.layers.len() - 1
    }
}
