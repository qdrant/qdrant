use crate::index::visited_pool::VisitedList;
use crate::types::{PointOffsetType, ScoreType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type LinkContainerRef<'a> = &'a [PointOffsetType];

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
struct SubVectorRef {
    pub offset: PointOffsetType,
    pub len: PointOffsetType,
    pub capacity: PointOffsetType,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct LinksContainer {
    points: Vec<SubVectorRef>,
    links_data: Vec<PointOffsetType>,
    levels_data: Vec<SubVectorRef>,
    free_link_parts: HashMap<PointOffsetType, Vec<SubVectorRef>>,
    free_level_parts: HashMap<PointOffsetType, Vec<SubVectorRef>>,
}

impl<'a> LinksContainer {
    pub fn new() -> LinksContainer {
        LinksContainer {
            points: Vec::new(),
            links_data: Vec::new(),
            levels_data: Vec::new(),
            free_link_parts: HashMap::new(),
            free_level_parts: HashMap::new(),
        }
    }

    pub fn reserve(&mut self, num_vectors: usize, links_capacity: usize) {
        let links_capacity = links_capacity.next_power_of_two();
        self.points.reserve(num_vectors);
        for _ in 0..num_vectors {
            let offset = self.links_data.len();
            self.links_data.resize(offset + links_capacity, 0);
            self.levels_data.push(SubVectorRef {
                offset: offset as PointOffsetType,
                len: 0,
                capacity: links_capacity as PointOffsetType,
            });
            self.points.push(SubVectorRef {
                offset: self.points.len() as PointOffsetType,
                len: 1,
                capacity: 1,
            });
        }
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn get_total_edges(&self) -> usize {
        let mut cnt: usize = 0;
        for point_id in 0..self.points.len() {
            for level in 0..self.points[point_id].len as usize {
                cnt += self.get_links(point_id as PointOffsetType, level).len();
            }
        }
        cnt
    }

    pub fn get_max_layers(&self) -> usize {
        (0..self.points.len() as PointOffsetType)
            .map(|i| self.point_level(i) + 1)
            .max()
            .unwrap()
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.points[point_id as usize].len as usize - 1
    }

    pub fn get_links(&self, point_id: PointOffsetType, level: usize) -> LinkContainerRef {
        let levels_ref = &self.points[point_id as usize];
        if level < levels_ref.len as usize {
            let links_ref = &self.levels_data[levels_ref.offset as usize + level];
            &self.links_data[links_ref.offset as usize..(links_ref.offset + links_ref.len) as usize]
        } else {
            &[]
        }
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, level: usize, links: LinkContainerRef) {
        let levels_ref = &mut self.points[point_id as usize];
        Self::realloc(
            &mut self.levels_data,
            levels_ref,
            level,
            &mut self.free_level_parts,
        );
        while levels_ref.len <= level as PointOffsetType {
            self.levels_data[(levels_ref.offset + levels_ref.len) as usize] =
                SubVectorRef::default();
            levels_ref.len += 1;
        }
        let links_ref = &mut self.levels_data[levels_ref.offset as usize + level];
        Self::realloc(
            &mut self.links_data,
            links_ref,
            links.len(),
            &mut self.free_link_parts,
        );
        links_ref.len = links.len() as PointOffsetType;
        self.links_data[links_ref.offset as usize..(links_ref.offset + links_ref.len) as usize]
            .clone_from_slice(links);
    }

    pub fn add_link(&mut self, point_id: PointOffsetType, level: usize, link: PointOffsetType) {
        let levels_ref = &mut self.points[point_id as usize];
        let links_ref = &mut self.levels_data[levels_ref.offset as usize + level];
        Self::realloc(
            &mut self.links_data,
            links_ref,
            links_ref.len as usize + 1,
            &mut self.free_link_parts,
        );
        self.links_data[(links_ref.offset + links_ref.len) as usize] = link;
        links_ref.len += 1;
    }

    pub fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        let point_id = point_id as usize;
        if self.points.len() <= point_id {
            self.points.resize(point_id + 1, SubVectorRef::default());
        }

        let levels_ref = &mut self.points[point_id];
        Self::realloc(
            &mut self.levels_data,
            levels_ref,
            level,
            &mut self.free_level_parts,
        );
        levels_ref.len = level as PointOffsetType;
    }

    pub fn merge_from_other(&mut self, other: LinksContainer, visited_list: &mut VisitedList) {
        panic!("not yet");
        /*
        if other.links_layers.len() > self.links_layers.len() {
            self.links_layers.resize(other.links_layers.len(), vec![]);
        }
        for (point_id, layers) in other.links_layers.into_iter().enumerate() {
            let current_layers = &mut self.links_layers[point_id];
            for (level, other_links) in layers.into_iter().enumerate() {
                if current_layers.len() <= level {
                    current_layers.push(other_links);
                } else {
                    visited_list.next_iteration();
                    let current_links = &mut current_layers[level];
                    current_links.iter().copied().for_each(|x| {
                        visited_list.check_and_update_visited(x);
                    });
                    for other_link in other_links
                        .into_iter()
                        .filter(|x| !visited_list.check_and_update_visited(*x))
                    {
                        current_links.push(other_link);
                    }
                }
            }
        }
        */
    }

    /// Connect new point to links, so that links contains only closest points
    pub fn connect_new_point<F>(
        &mut self,
        point_id: PointOffsetType,
        level: usize,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score_internal: F,
    ) where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score_internal(target_point_id, new_point_id);

        let mut id_to_insert;
        let links_len;
        {
            let links = self.get_links(point_id, level);
            links_len = links.len();
            id_to_insert = links_len;
            for (i, &item) in links.iter().enumerate() {
                let target_to_link = score_internal(target_point_id, item);
                if target_to_link < new_to_target {
                    id_to_insert = i;
                    break;
                }
            }
        }

        let levels_ref = &mut self.points[point_id as usize];
        let links_ref = &mut self.levels_data[levels_ref.offset as usize + level];

        if links_len >= level_m && id_to_insert != links_len {
            links_ref.len -= 1; // pop last element
        }

        if links_len < level_m || (links_len >= level_m && id_to_insert != links_len) {
            Self::realloc(
                &mut self.links_data,
                links_ref,
                links_ref.len as usize + 1,
                &mut self.free_link_parts,
            );
            self.links_data
                [links_ref.offset as usize + id_to_insert..(links_ref.offset + links_ref.len + 1) as usize]
                .rotate_right(1);
            self.links_data[links_ref.offset as usize + id_to_insert] = new_point_id;
            links_ref.len += 1;
        }
    }

    fn realloc<T>(
        data: &mut Vec<T>,
        subvec: &mut SubVectorRef,
        required_capacity: usize,
        free_parts: &mut HashMap<PointOffsetType, Vec<SubVectorRef>>,
    ) where
        T: Default + Clone,
    {
        let required_capacity = usize::next_power_of_two(required_capacity) as PointOffsetType;
        if subvec.capacity >= required_capacity {
            return;
        }

        let old_part = SubVectorRef {
            offset: subvec.offset,
            len: 0,
            capacity: subvec.capacity,
        };
        if subvec.capacity > 0 {
            match free_parts.get_mut(&old_part.capacity) {
                Some(free_datas) => {
                    free_datas.push(old_part.clone());
                }
                None => {
                    free_parts.insert(old_part.capacity, vec![old_part.clone()]);
                }
            }
        }

        let mut new_part = SubVectorRef {
            offset: data.len() as PointOffsetType,
            len: 0,
            capacity: required_capacity,
        };
        if let Some(free_datas) = free_parts.get_mut(&required_capacity) {
            if let Some(free) = free_datas.pop() {
                new_part = free;
            }
        }

        let required_data_len = (new_part.offset + new_part.capacity) as usize;
        if data.len() < required_data_len {
            data.resize(required_data_len, T::default());
        }

        if old_part.offset > new_part.offset {
            let (dst, src) = data.as_mut_slice().split_at_mut(old_part.offset as usize);
            dst[new_part.offset as usize..(new_part.offset + subvec.len) as usize]
                .clone_from_slice(&src[..subvec.len as usize]);
        } else {
            let (src, dst) = data.as_mut_slice().split_at_mut(new_part.offset as usize);
            dst[..subvec.len as usize].clone_from_slice(
                &src[old_part.offset as usize..(old_part.offset + subvec.len) as usize],
            );
        }
        subvec.offset = new_part.offset;
        subvec.capacity = new_part.capacity;
    }
}
