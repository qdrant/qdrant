use crate::types::PointOffsetType;

#[derive(Debug, Default)]
pub struct SimpleLinksContainer {
    links_data: Vec<Vec<Vec<PointOffsetType>>>,
}

impl SimpleLinksContainer {
    pub fn new(num_points: usize) -> Self {
        let mut links_data: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let empty_level = vec![];
        links_data.resize(num_points, vec![empty_level]);

        Self { links_data }
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        level: usize,
        links: &[PointOffsetType],
    ) {
        self.links_data[point_id as usize][level].clear();
        self.links_data[point_id as usize][level].extend_from_slice(links);
    }

    pub fn get_links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        &self.links_data[point_id as usize][level]
    }
}
