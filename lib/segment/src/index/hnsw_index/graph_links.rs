use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

const MAX_U24: u32 = 256 * 256 * 256 - 1;

/*
Links data for whole layer.

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
links offset = lvl offset + reindex[point_id]
*/
#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GraphLinks {
    links: Vec<u8>,
    offsets: Vec<usize>,
    offsets_start: Vec<usize>,
    reindex: Vec<PointOffsetType>,
}

impl GraphLinks {
    // Convert from graph layers builder links
    // `Vec<Vec<Vec<_>>>` means:
    // vector of points -> vector of layers for specific point -> vector of links for specific point and layer
    pub fn from_vec(edges: &Vec<Vec<Vec<PointOffsetType>>>, m0: usize, m: usize) -> Self {
        if edges.len() == 0 {
            return Self::default();
        }

        // create map from offsets to point id. sort by max layer and use this map to build `GraphLinks::reindex`
        let mut back_index = (0..edges.len() as PointOffsetType).collect::<Vec<PointOffsetType>>();
        back_index.sort_unstable_by_key(|&i| -(edges[i as usize].len() as i32));

        // reindex is map from point id to index in `LayerData.offsets`
        let mut reindex = vec![0; edges.len()];
        for i in 0..edges.len() {
            reindex[back_index[i] as usize] = i as PointOffsetType;
        }

        let mut graph_links = GraphLinks {
            links: Vec::new(),
            offsets: vec![0],
            offsets_start: Vec::new(),
            reindex,
        };

        let max_levels = edges[back_index[0] as usize].len();
        for level in 0..max_levels {
            let m = if level == 0 { m0 } else { m };
            for i in 0..edges.len() {
                let reindexed = if level == 0 {
                    i
                } else {
                    back_index[i] as usize
                };
                if level >= edges[reindexed].len() {
                    break;
                }
                let mut links = edges[reindexed][level].clone();
                let links_count = links.len();
                let first_links = &mut links[0..std::cmp::min(m, links_count)];
                first_links.sort();
                let u24_count = first_links.binary_search(&MAX_U24).err().unwrap();
                graph_links.links.push(u24_count as u8);
                for i in 0..u24_count {
                    let link = links[i];
                    graph_links.links.push(((link >> 16) % 256) as u8);
                    graph_links.links.push(((link >> 8) % 256) as u8);
                    graph_links.links.push((link % 256) as u8);
                }
                for i in u24_count..links_count {
                    let link = links[i];
                    graph_links.links.push(((link >> 24) % 256) as u8);
                    graph_links.links.push(((link >> 16) % 256) as u8);
                    graph_links.links.push(((link >> 8) % 256) as u8);
                    graph_links.links.push((link % 256) as u8);
                }
                graph_links.offsets.push(graph_links.links.len());
            }
            graph_links
                .offsets_start
                .push(graph_links.offsets.len() - 1);
        }
        graph_links.offsets_start.pop();
        graph_links
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

    pub fn links(&self, point_id: PointOffsetType, level: usize) -> Vec<PointOffsetType> {
        if level == 0 {
            self.links_layer0(point_id)
        } else {
            self.links_impl(point_id, level)
        }
    }

    pub fn map_links<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType),
    {
        if level == 0 {
            self.map_links_layer0(point_id, f)
        } else {
            self.map_links_impl(point_id, level, f)
        }
    }

    pub fn num_points(&self) -> usize {
        self.offsets_start.first().copied().unwrap_or(0)
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed = self.reindex[point_id as usize] as usize;
        for level in 1.. {
            if level > self.offsets_start.len() {
                return level - 1;
            }
            let layer_offsets_start = self.offsets_start[level - 1];
            let layer_offsets_end = if level == self.offsets_start.len() {
                self.offsets.len() - 1
            } else {
                self.offsets_start[level]
            };
            if layer_offsets_start + reindexed >= layer_offsets_end {
                return level - 1;
            }
        }
        unreachable!()
    }

    fn links_layer0(&self, point_id: PointOffsetType) -> Vec<PointOffsetType> {
        let start = self.offsets[point_id as usize];
        let end = self.offsets[point_id as usize + 1];
        Self::get_links(&self.links[start..end])
    }

    fn links_impl(&self, point_id: PointOffsetType, level: usize) -> Vec<PointOffsetType> {
        debug_assert!(level > 0);
        let point_id = self.reindex[point_id as usize] as usize;
        let layer_offsets_start = self.offsets_start[level - 1];
        let start = self.offsets[layer_offsets_start + point_id as usize];
        let end = self.offsets[layer_offsets_start + point_id as usize + 1];
        Self::get_links(&self.links[start..end])
    }

    fn get_links(data: &[u8]) -> Vec<PointOffsetType> {
        let mut result = Vec::new();
        let u24_count = data[0] as usize;

        let mut i = 1;
        for _ in 0..u24_count {
            let link = (data[i] as PointOffsetType) << 16
                | (data[i + 1] as PointOffsetType) << 8
                | (data[i + 2] as PointOffsetType);
            result.push(link);
            i += 3;
        }

        while i < data.len() {
            let link = (data[i] as PointOffsetType) << 24
                | (data[i + 1] as PointOffsetType) << 16
                | (data[i + 2] as PointOffsetType) << 8
                | (data[i + 3] as PointOffsetType);
            result.push(link);
            i += 4;
        }

        result
    }

    fn map_links_layer0<F>(&self, point_id: PointOffsetType, f: F)
    where
        F: FnMut(PointOffsetType),
    {
        let start = self.offsets[point_id as usize];
        let end = self.offsets[point_id as usize + 1];
        Self::map_get_links(&self.links[start..end], f);
    }

    fn map_links_impl<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType),
    {
        debug_assert!(level > 0);
        let point_id = self.reindex[point_id as usize] as usize;
        let layer_offsets_start = self.offsets_start[level - 1];
        let start = self.offsets[layer_offsets_start + point_id as usize];
        let end = self.offsets[layer_offsets_start + point_id as usize + 1];
        Self::map_get_links(&self.links[start..end], f);
    }

    fn map_get_links<F>(data: &[u8], mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        let u24_count = data[0] as usize;
        let mut i = 1;
        for _ in 0..u24_count {
            let link = (data[i] as PointOffsetType) << 16
                | (data[i + 1] as PointOffsetType) << 8
                | (data[i + 2] as PointOffsetType);
            f(link);
            i += 3;
        }

        while i < data.len() {
            let link = (data[i] as PointOffsetType) << 24
                | (data[i + 1] as PointOffsetType) << 16
                | (data[i + 2] as PointOffsetType) << 8
                | (data[i + 3] as PointOffsetType);
            f(link);
            i += 4;
        }
    }
}
