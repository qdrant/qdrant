//! Structures for fast and tread-safe way to check if some points were visited or not

use crate::types::PointOffsetType;
use parking_lot::RwLock;

/// Max number of visited lists to preserve in memory
/// If more than this number of concurrent requests occurred - new list will be created dynamically,
/// but will be deleted right after query finishes.
/// Implemented in order to limit memory leak
const POOL_KEEP_LIMIT: usize = 16;

/// Visited list reuses same memory to keep track of visited points ids among multiple consequent queries
///
/// It stores the sequence number of last processed operation next to the point ID, which allows to avoid memory allocation
/// and re-use same counter for multiple queries.
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

    /// Return `true` if visited
    pub fn check(&self, point_id: PointOffsetType) -> bool {
        self.visit_counters
            .get(point_id as usize)
            .map(|x| *x >= self.current_iter)
            .unwrap_or(false)
    }

    /// Updates visited list
    /// return `true` if point was visited before
    pub fn check_and_update_visited(&mut self, point_id: PointOffsetType) -> bool {
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

/// Keeps a list of `VisitedList` which could be requested and released from multiple threads
///
/// If there are more requests than lists - creates a new list, but only keeps max defined amount.
#[derive(Debug)]
pub struct VisitedPool {
    pool: RwLock<Vec<VisitedList>>,
}

impl VisitedPool {
    pub fn new() -> Self {
        VisitedPool {
            pool: RwLock::new(vec![]),
        }
    }

    pub fn get(&self, num_points: usize) -> VisitedList {
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
            pool.push(visited_list);
        }
    }
}

impl Default for VisitedPool {
    fn default() -> Self {
        VisitedPool::new()
    }
}
