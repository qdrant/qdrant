use crate::types::PointOffsetType;

#[derive(Debug, Default)]
pub struct BaseLinksContainer {
    links_data: Vec<PointOffsetType>,
    links_per_record: usize,
    num_records: usize,
}

impl BaseLinksContainer {
    #[inline]
    fn links_offset(&self, point_id: PointOffsetType) -> usize {
        point_id as usize * self.links_per_record
    }

    pub fn new(links_per_record: usize, num_records: usize) -> BaseLinksContainer {
        let mut links_data: Vec<PointOffsetType> = Default::default();

        links_data.resize(num_records * (links_per_record + 1), 0);

        BaseLinksContainer {
            links_data,
            links_per_record,
            num_records,
        }
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        let mut offset = self.links_offset(point_id);
        self.links_data[offset] = links.len() as PointOffsetType;
        offset += 1;
        for link in links {
            self.links_data[offset] = *link;
            offset += 1;
        }
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        let offset = self.links_offset(point_id);
        let num_links = self.links_data[offset] as usize;
        &self.links_data[(offset + 1)..(offset + 1 + num_links)]
    }
}
