use crate::types::PointOffsetType;

#[derive(Debug, Default)]
pub struct SimpleLinksContainerNoLevel {
    links_data: Vec<Vec<PointOffsetType>>,
}

impl SimpleLinksContainerNoLevel {
    pub fn new(num_points: usize) -> Self {
        let mut links_data: Vec<Vec<PointOffsetType>> = vec![];
        links_data.resize(num_points, vec![]);

        Self { links_data }
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        self.links_data[point_id as usize].clear();
        self.links_data[point_id as usize].extend_from_slice(links);
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        &self.links_data[point_id as usize]
    }
}
