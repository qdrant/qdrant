use crate::types::PointOffsetType;
use std::collections::VecDeque;
use parking_lot::RwLock;
use std::sync::Arc;

/// Max number of visited lists to preserve in memory
/// If more than this number of concurrent requests occurred - new list will be created dynamically,
/// but will be deleted right after query finishes.
/// Implemented in order to limit memory leak
const POOL_KEEP_LIMIT: usize = 16;

#[derive(Debug)]
pub struct VisitedList {
    current_iter: usize,
    visit_counters: Vec<usize>,
}


impl VisitedList {
    pub fn new(num_points: usize) -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![0; num_points],
        }
    }

    /// Updates visited list, return if point was visited before
    pub fn update_visited(&mut self, point_id: PointOffsetType) -> bool {
        let idx = point_id as usize;
        if idx >= self.visit_counters.len() {
            self.visit_counters.resize(idx + 1, 0);
        }
        let prev_value = self.visit_counters[idx];
        self.visit_counters[idx] = self.current_iter;
        prev_value >= self.current_iter
    }

    pub fn next_iteration(&mut self) {
        self.current_iter += 1;
    }
}


#[derive(Debug)]
pub struct VisitedPool {
    pool: RwLock<Vec<VisitedList>>
}


impl VisitedPool {
    pub fn new() -> Self {
        VisitedPool {
            pool: RwLock::new(vec![])
        }
    }

    pub fn get(&self, num_points: usize) -> VisitedList{
        match self.pool.write().pop() {
            None => VisitedList::new(num_points),
            Some(mut vl) => {
                vl.next_iteration();
                vl
            }
        }
    }

    pub fn return_back(&self, visited_list: VisitedList) {
        let mut pool = self.pool.write();
        if pool.len() < POOL_KEEP_LIMIT {
            pool.push(visited_list)
        }
    }
}

impl Default for VisitedPool {
    fn default() -> Self {
        VisitedPool::new()
    }
}