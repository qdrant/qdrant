use crate::types::PointOffsetType;


#[derive(Debug)]
pub struct VisitedPool {
    current_iter: usize,
    visit_counters: Vec<usize>,
}


impl VisitedPool {
    pub fn new(num_points: usize) -> Self {
        VisitedPool {
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