use crate::index::visited_pool::VisitedList;
use crate::types::{PointOffsetType, ScoreType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type LinkContainerRef<'a> = &'a [PointOffsetType];

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
struct SubVectorRef {
    pub offset: usize,
    pub len: usize,
    pub capacity: usize,
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct VecVec<T> {
    data: Vec<T>,
    free_parts: HashMap<usize, Vec<SubVectorRef>>,
}

fn copy_within_a_slice<T: Clone>(v: &mut [T], from: usize, to: usize, len: usize) {
    if from > to {
        let (dst, src) = v.split_at_mut(from);
        dst[to..to + len].clone_from_slice(&src[..len]);
    } else {
        let (src, dst) = v.split_at_mut(to);
        dst[..len].clone_from_slice(&src[from..from + len]);
    }
}

impl<T> VecVec<T>
where
    T: Clone + Default,
{
    pub fn new() -> VecVec<T> {
        VecVec {
            data: Vec::new(),
            free_parts: HashMap::new(),
        }
    }

    pub fn get(&self, subvec: &SubVectorRef, idx: usize) -> &T {
        &self.data[subvec.offset + idx]
    }

    pub fn get_slice(&self, subvec: &SubVectorRef) -> &[T] {
        &self.data[subvec.offset..subvec.offset + subvec.len]
    }

    pub fn set(&mut self, subvec: &SubVectorRef, idx: usize, value: T) {
        self.data[subvec.offset + idx] = value;
    }

    pub fn pop(&mut self, subvec: &mut SubVectorRef) {
        if subvec.len > 0 {
            subvec.len -= 1;
        }
    }

    pub fn insert(&mut self, subvec: &mut SubVectorRef, pos: usize, value: T) {
        if subvec.capacity == subvec.len {
            self.realloc(subvec);
        }
        self.data[subvec.offset + pos..subvec.offset + subvec.len + 1].rotate_right(1);
        self.data[subvec.offset + pos] = value;
        subvec.len += 1;
    }

    pub fn push(&mut self, subvec: &mut SubVectorRef, value: T) {
        if subvec.capacity == subvec.len {
            self.realloc(subvec);
        }
        self.data[subvec.offset + subvec.len] = value;
        subvec.len += 1;
    }

    pub fn clone_from_slice(&mut self, subvec: &mut SubVectorRef, values: &[T]) {
        while subvec.capacity < values.len() {
            self.realloc(subvec);
        }
        self.data[subvec.offset..subvec.offset + values.len()].clone_from_slice(values);
        subvec.len = values.len();
    }

    pub fn reserve(&mut self, subvec: &mut SubVectorRef, min_capacity: usize) {
        while subvec.capacity < min_capacity {
            self.realloc(subvec);
        }
    }

    fn realloc(&mut self, subvec: &mut SubVectorRef) {
        let old_part = SubVectorRef {
            offset: subvec.offset,
            len: 0,
            capacity: subvec.capacity,
        };
        if subvec.capacity > 0 {
            match self.free_parts.get_mut(&old_part.capacity) {
                Some(free_datas) => {
                    free_datas.push(old_part.clone());
                }
                None => {
                    self.free_parts
                        .insert(old_part.capacity, vec![old_part.clone()]);
                }
            }
        }

        let new_capacity = if subvec.capacity > 0 {
            subvec.capacity * 2
        } else {
            4
        };
        let mut new_part = SubVectorRef {
            offset: self.data.len(),
            len: 0,
            capacity: new_capacity,
        };
        if let Some(free_datas) = self.free_parts.get_mut(&new_capacity) {
            if let Some(free) = free_datas.pop() {
                new_part = free;
            }
        }

        if self.data.len() < new_part.offset + new_part.capacity {
            self.data
                .resize(new_part.offset + new_part.capacity, T::default());
        }
        copy_within_a_slice(&mut self.data, old_part.offset, new_part.offset, subvec.len);
        subvec.offset = new_part.offset;
        subvec.capacity = new_part.capacity;
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct LinksContainer {
    points: Vec<SubVectorRef>,
    links_data: VecVec<PointOffsetType>,
    levels_data: VecVec<SubVectorRef>,
}

impl<'a> LinksContainer {
    pub fn new() -> LinksContainer {
        LinksContainer {
            points: Vec::new(),
            links_data: VecVec::new(),
            levels_data: VecVec::new(),
        }
    }

    pub fn reserve(&mut self, num_vectors: usize, links_capacity: usize) {
        self.points.resize(num_vectors, SubVectorRef::default());
        for i in 0..self.points.len() {
            let mut links = SubVectorRef::default();
            self.links_data.reserve(&mut links, links_capacity);
            self.levels_data.push(&mut self.points[i], links);
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
        for levels in &self.points {
            let levels = self.levels_data.get_slice(levels);
            cnt += levels.iter().map(|x| x.len).sum::<usize>();
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
        self.points[point_id as usize].len - 1
    }

    pub fn get_links(&self, point_id: PointOffsetType, level: usize) -> LinkContainerRef {
        let levels_ref = &self.points[point_id as usize];
        if levels_ref.len != 0 {
            let links = self.levels_data.get(levels_ref, level);
            self.links_data.get_slice(links)
        } else {
            &[]
        }
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, level: usize, links: LinkContainerRef) {
        let mut levels_ref = self.points[point_id as usize].clone();
        while levels_ref.len <= level {
            let links = SubVectorRef::default();
            self.levels_data.push(&mut levels_ref, links);
        }
        let mut links_ref = self.levels_data.get(&levels_ref, level).clone();
        self.links_data.clone_from_slice(&mut links_ref, links);
        self.levels_data.set(&mut levels_ref, level, links_ref);
        self.points[point_id as usize] = levels_ref;
    }

    pub fn add_links(&mut self, point_id: PointOffsetType, level: usize, links: LinkContainerRef) {
        let levels_ref = &self.points[point_id as usize];
        let mut links_ref = self.levels_data.get(levels_ref, level).clone();
        for link in links {
            self.links_data.push(&mut links_ref, *link);
        }
        self.levels_data.set(levels_ref, level, links_ref);
    }

    pub fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        let point_id = point_id as usize;
        if self.points.len() <= point_id {
            self.points.resize(point_id + 1, SubVectorRef::default());
        }

        let mut levels_ref = self.points[point_id].clone();
        for _ in 0..level {
            self.levels_data
                .push(&mut levels_ref, SubVectorRef::default());
        }
        self.points[point_id] = levels_ref;
    }

    pub fn merge_from_other(&mut self, other: LinksContainer, visited_list: &mut VisitedList) {
        //panic!("merge_from_other");
        if other.points.len() > self.points.len() {
            self.points
                .resize(other.points.len(), SubVectorRef::default());
        }

        for (point_id, layers) in other.points.iter().enumerate() {
            let mut current_layers = self.points[point_id].clone();
            let layers = other.levels_data.get_slice(layers);
            for (level, other_links) in layers.iter().enumerate() {
                let other_links = other.links_data.get_slice(other_links);
                if current_layers.len <= level {
                    let mut added_links = SubVectorRef::default();
                    self.links_data
                        .clone_from_slice(&mut added_links, other_links);
                    self.levels_data.push(&mut current_layers, added_links);
                } else {
                    visited_list.next_iteration();
                    {
                        let current_links = self.get_links(point_id as PointOffsetType, level);
                        current_links.iter().copied().for_each(|x| {
                            visited_list.check_and_update_visited(x);
                        });
                    }
                    let mut links = self.levels_data.get(&current_layers, level).clone();
                    for other_link in other_links
                        .iter()
                        .copied()
                        .filter(|x| !visited_list.check_and_update_visited(*x))
                    {
                        self.links_data.push(&mut links, other_link);
                    }
                    self.levels_data.set(&current_layers, level, links);
                }
            }
            self.points[point_id] = current_layers;
        }
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

        let mut links_ref = self
            .levels_data
            .get(&self.points[point_id as usize], level)
            .clone();
        if links_len < level_m {
            self.links_data
                .insert(&mut links_ref, id_to_insert, new_point_id);
        } else if id_to_insert != links_len {
            self.links_data.pop(&mut links_ref);
            self.links_data
                .insert(&mut links_ref, id_to_insert, new_point_id);
        }
        self.levels_data
            .set(&self.points[point_id as usize], level, links_ref);
    }
}
