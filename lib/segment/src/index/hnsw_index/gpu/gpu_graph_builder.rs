use std::collections::BinaryHeap;
use std::sync::Arc;

use num_traits::float::FloatCore;
use rand::Rng;

use super::gpu_links::GpuLinks;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{ScoredPointOffset, VectorStorage, VectorStorageEnum};

pub struct GpuGraphBuilder {
    pub graph_layers_builder: GraphLayersBuilder,
    pub gpu_instance: Arc<gpu::Instance>,
    pub gpu_device: Arc<gpu::Device>,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
    pub max_level: usize,
    pub entries: Vec<Option<PointOffsetType>>,
    //pub gpu_search_context: GpuSearchContext,
    //pub gpu_builder_context: GpuBuilderContext,
}

impl GpuGraphBuilder {
    pub fn new<R>(
        rng: &mut R,
        vector_storage: &VectorStorageEnum,
        m: usize,
        m0: usize,
        ef_construct: usize,
        entry_points_num: usize,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let num_vectors = vector_storage.total_vector_count();
        let mut graph_layers_builder = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            true,
            true,
        );

        let point_levels = (0..num_vectors)
            .map(|_| graph_layers_builder.get_random_layer(rng))
            .collect::<Vec<_>>();
        let max_level = point_levels.iter().max().cloned().unwrap();

        for idx in 0..(num_vectors as PointOffsetType) {
            graph_layers_builder.set_levels(idx, point_levels[idx as usize]);
        }

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let gpu_device = Arc::new(
            gpu::Device::new(gpu_instance.clone(), gpu_instance.vk_physical_devices[0]).unwrap(),
        );
        let gpu_vector_storage = GpuVectorStorage::new(gpu_device.clone(), vector_storage).unwrap();
        let gpu_links =
            GpuLinks::new(gpu_device.clone(), m, ef_construct, m0, num_vectors).unwrap();

        let mut entry_points = EntryPoints::new(entry_points_num);
        let entries = (0..num_vectors as PointOffsetType)
            .map(|idx| {
                entry_points
                    .new_point(idx, point_levels[idx as usize], |_| true)
                    .map(|e| e.point_id)
            })
            .collect::<Vec<_>>();

        Self {
            graph_layers_builder,
            gpu_instance,
            gpu_device,
            gpu_vector_storage,
            gpu_links,
            max_level,
            entries,
        }
    }

    pub fn build(&mut self) {
        for level in (0..=self.max_level).rev() {
            self.build_level_cpu(level, self.graph_layers_builder.get_m(level));
        }
    }

    pub fn build_level_cpu(&mut self, level: usize, level_m: usize) {
        for idx in 0..self.num_vectors() {
            if let Some(entry) = self.entries[idx].clone() {
                let entry_level = self.get_point_level(entry);
                let point_level = self.get_point_level(idx as PointOffsetType);
                let scored_entry = ScoredPointOffset {
                    idx: entry,
                    score: self.score(idx as PointOffsetType, entry),
                };
                if level > entry_level && entry_level >= point_level {
                    let new_entry = self.search_entry(idx as PointOffsetType, scored_entry);
                    self.entries[idx] = Some(new_entry.idx);
                } else if entry_level >= level {
                    let new_entry = self.link(idx as PointOffsetType, level_m, scored_entry);
                    self.entries[idx] = Some(new_entry.idx);
                }
            }
        }
    }

    pub fn link(
        &mut self,
        point_id: PointOffsetType,
        level_m: usize,
        entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let nearest_points = self.search(point_id, entry);

        let next_entry = nearest_points.iter().copied().max().unwrap_or(entry);

        let links = self.select_with_heuristic(nearest_points, level_m);
        self.set_links(point_id, &links);
        for other_point in links {
            let other_point_links = self.get_links(other_point);
            if other_point_links.len() < level_m {
                // If linked point is lack of neighbours
                let mut other_point_links = other_point_links.to_vec();
                other_point_links.push(point_id);
                self.set_links(other_point, &other_point_links);
            } else {
                let mut candidates =
                    FixedLengthPriorityQueue::<ScoredPointOffset>::new(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: point_id,
                    score: self.score(point_id, other_point),
                });
                for other_point_link in other_point_links.iter().take(level_m).copied() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(other_point_link, other_point),
                    });
                }
                let selected_candidates = self.select_with_heuristic(candidates, level_m);
                self.set_links(other_point, &selected_candidates);
            }
        }
        next_entry
    }

    pub fn select_with_heuristic(
        &self,
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
    ) -> Vec<PointOffsetType> {
        let mut result_list = vec![];
        result_list.reserve(m);
        for current_closest in candidates.into_vec() {
            if result_list.len() >= m {
                break;
            }
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected = self.score(current_closest.idx, selected_point);
                if dist_to_already_selected > current_closest.score {
                    is_good = false;
                    break;
                }
            }
            if is_good {
                result_list.push(current_closest.idx);
            }
        }

        result_list
    }

    pub fn search(
        &self,
        id: PointOffsetType,
        level_entry: ScoredPointOffset,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let ef = self.graph_layers_builder.ef_construct;
        let mut visited_list = self
            .graph_layers_builder
            .visited_pool
            .get(self.num_vectors());
        visited_list.check_and_update_visited(level_entry.idx);

        let mut nearest = FixedLengthPriorityQueue::<ScoredPointOffset>::new(ef);
        nearest.push(level_entry);
        let mut candidates = BinaryHeap::<ScoredPointOffset>::from_iter([level_entry]);

        while let Some(candidate) = candidates.pop() {
            let lower_bound = match nearest.top() {
                None => ScoreType::min_value(),
                Some(worst_of_the_best) => worst_of_the_best.score,
            };
            if candidate.score < lower_bound {
                break;
            }

            let links = self.get_links(candidate.idx);
            for &link in links.iter() {
                if !visited_list.check_and_update_visited(link) {
                    let score = self.score(link, id);
                    Self::process_candidate(
                        &mut nearest,
                        &mut candidates,
                        ScoredPointOffset { idx: link, score },
                    )
                }
            }
        }

        for &existing_link in self.get_links(id) {
            if !visited_list.check(existing_link) {
                Self::process_candidate(
                    &mut nearest,
                    &mut candidates,
                    ScoredPointOffset {
                        idx: existing_link,
                        score: self.score(id, existing_link),
                    },
                );
            }
        }

        self.graph_layers_builder
            .visited_pool
            .return_back(visited_list);
        nearest
    }

    pub fn process_candidate(
        nearest: &mut FixedLengthPriorityQueue<ScoredPointOffset>,
        candidates: &mut BinaryHeap<ScoredPointOffset>,
        score_point: ScoredPointOffset,
    ) {
        let was_added = match nearest.push(score_point) {
            None => true,
            Some(removed) => removed.idx != score_point.idx,
        };
        if was_added {
            candidates.push(score_point);
        }
    }

    pub fn search_entry(
        &self,
        id: PointOffsetType,
        mut entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            for &link in self.get_links(entry.idx) {
                let score = self.score(link, id);
                if score > entry.score {
                    changed = true;
                    entry = ScoredPointOffset { idx: link, score };
                }
            }
        }
        entry
    }

    pub fn get_m(&self, level: usize) -> usize {
        self.graph_layers_builder.get_m(level)
    }

    pub fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.graph_layers_builder.links_layers[point_id as usize].len()
    }

    pub fn num_vectors(&self) -> usize {
        self.graph_layers_builder.links_layers.len()
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        self.gpu_links.get_links(point_id)
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        self.gpu_links.set_links(point_id, links);
    }

    pub fn score(&self, _a: PointOffsetType, _b: PointOffsetType) -> ScoreType {
        todo!()
    }
}
