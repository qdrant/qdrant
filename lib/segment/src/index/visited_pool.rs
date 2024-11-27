//! Structures for fast and tread-safe way to check if some points were visited or not

use common::defaults::POOL_KEEP_LIMIT;
use common::types::PointOffsetType;
use parking_lot::RwLock;

/// Visited list handle is an owner of the `VisitedList`, which is returned by `VisitedPool` and returned back to it
#[derive(Debug)]
pub struct VisitedListHandle<'a> {
    pool: &'a VisitedPool,
    visited_list: VisitedList,
}

/// Visited list reuses same memory to keep track of visited points ids among multiple consequent queries
///
/// It stores the sequence number of last processed operation next to the point ID, which allows to avoid memory allocation
/// and reuse same counter for multiple queries.
#[derive(Debug)]
struct VisitedList {
    current_iter: u8,
    visit_counters: Vec<u8>,
}

impl Default for VisitedList {
    fn default() -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![],
        }
    }
}

impl VisitedList {
    fn new(num_points: usize) -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![0; num_points],
        }
    }
}

impl Drop for VisitedListHandle<'_> {
    fn drop(&mut self) {
        self.pool
            .return_back(std::mem::take(&mut self.visited_list));
    }
}

impl<'a> VisitedListHandle<'a> {
    fn new(pool: &'a VisitedPool, data: VisitedList) -> Self {
        VisitedListHandle {
            pool,
            visited_list: data,
        }
    }

    /// Return `true` if visited
    pub fn check(&self, point_id: PointOffsetType) -> bool {
        self.visited_list
            .visit_counters
            .get(point_id as usize)
            .is_some_and(|x| *x == self.visited_list.current_iter)
    }

    /// Updates visited list
    /// return `true` if point was visited before
    pub fn check_and_update_visited(&mut self, point_id: PointOffsetType) -> bool {
        let idx = point_id as usize;
        if idx >= self.visited_list.visit_counters.len() {
            self.visited_list.visit_counters.resize(idx + 1, 0);
        }
        std::mem::replace(
            &mut self.visited_list.visit_counters[idx],
            self.visited_list.current_iter,
        ) == self.visited_list.current_iter
    }

    pub fn next_iteration(&mut self) {
        self.visited_list.current_iter = self.visited_list.current_iter.wrapping_add(1);
        if self.visited_list.current_iter == 0 {
            self.visited_list.current_iter = 1;
            self.visited_list.visit_counters.fill(0);
        }
    }

    fn resize(&mut self, num_points: usize) {
        // `self.current_iter` is never 0, so it's safe to use 0 as a default
        // value.
        self.visited_list.visit_counters.resize(num_points, 0);
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
            pool: RwLock::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self, num_points: usize) -> VisitedListHandle {
        // If there are more concurrent requests, a new temporary list is created dynamically.
        // This limit is implemented to prevent memory leakage.
        match self.pool.write().pop() {
            None => VisitedListHandle::new(self, VisitedList::new(num_points)),
            Some(data) => {
                let mut visited_list = VisitedListHandle::new(self, data);
                visited_list.resize(num_points);
                visited_list.next_iteration();
                visited_list
            }
        }
    }

    fn return_back(&self, data: VisitedList) {
        let mut pool = self.pool.write();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for VisitedPool {
    fn default() -> Self {
        VisitedPool::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visited_list() {
        let pool = VisitedPool::new();
        let mut visited_list = pool.get(10);

        for _ in 0..2 {
            assert!(!visited_list.check(0));
            assert!(!visited_list.check_and_update_visited(0));
            assert!(visited_list.check(0));

            assert!(visited_list.check_and_update_visited(0));
            assert!(visited_list.check(0));

            for _ in 0..(u8::MAX as usize * 2 + 10) {
                visited_list.next_iteration();
                assert!(!visited_list.check(0));
            }
        }
    }
}
