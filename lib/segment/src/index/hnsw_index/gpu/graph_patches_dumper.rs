use common::types::PointOffsetType;
use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub enum GraphChangeDump {
    UpdateLinks {
        point_id: PointOffsetType,
        dst_point_id: PointOffsetType,
        new_links: Vec<PointOffsetType>,
    },
    UpdateEntry {
        point_id: PointOffsetType,
        new_entry: PointOffsetType,
    },
}

#[derive(Default, Debug)]
pub struct GraphPatchesDumper {
    pub other: Option<Box<GraphPatchesDumper>>,
    pub levels: Mutex<Vec<Vec<GraphChangeDump>>>,
}

impl GraphPatchesDumper {
    pub fn new(other: Option<Box<GraphPatchesDumper>>) -> Self {
        if let Some(other) = &other {
            let mut levels = other.levels.lock();
            for level in levels.iter_mut() {
                level.reverse();
            }
        }
        Self {
            other,
            levels: Default::default(),
        }
    }

    pub fn update_links(
        &self,
        level: usize,
        point_id: PointOffsetType,
        dst_point_id: PointOffsetType,
        new_links: &[PointOffsetType],
    ) -> bool {
        let mut levels = self.levels.lock();
        while level >= levels.len() {
            levels.push(Vec::new());
        }
        levels[level].push(GraphChangeDump::UpdateLinks {
            point_id,
            dst_point_id,
            new_links: new_links.to_vec(),
        });

        if let Some(other) = &self.other {
            let mut other_levels = other.levels.lock();
            if level < other_levels.len() {
                if let Some(GraphChangeDump::UpdateLinks {
                    point_id: other_point_id,
                    dst_point_id: other_dst_point_id,
                    new_links: other_new_links,
                }) = other_levels[level].pop()
                {
                    if point_id == other_point_id
                        && dst_point_id == other_dst_point_id
                        && &other_new_links == new_links
                    {
                        true
                    } else {
                        println!(
                            "ERROR! Graph patches mismatch. REF: point_id={}, update_id={}, links={:?}. GOT: point_id={}, update_id={}, links={:?}.",
                            other_point_id, other_dst_point_id, other_new_links, point_id, dst_point_id, new_links,
                        );
                        false
                    }
                } else {
                    println!(
                        "ERROR! Graph patches mismatch. No changes or wrong type. REF: point_id={}, update_id={}",
                        point_id, dst_point_id);
                    false
                }
            } else {
                println!(
                    "ERROR! Graph patches no level. No changes or wrong type. REF: point_id={}, update_id={}",
                    point_id, dst_point_id);
                false
            }
        } else {
            true
        }
    }

    pub fn update_entry(
        &self,
        level: usize,
        point_id: PointOffsetType,
        new_entry: PointOffsetType,
    ) -> bool {
        let mut levels = self.levels.lock();
        while level >= levels.len() {
            levels.push(Vec::new());
        }
        levels[level].push(GraphChangeDump::UpdateEntry {
            point_id,
            new_entry,
        });

        if let Some(other) = &self.other {
            let mut other_levels = other.levels.lock();
            if level < other_levels.len() {
                if let Some(GraphChangeDump::UpdateEntry {
                    point_id: other_point_id,
                    new_entry: other_new_entry,
                }) = other_levels[level].pop()
                {
                    if point_id == other_point_id && new_entry == other_new_entry {
                        true
                    } else {
                        println!(
                            "ERROR! Graph new entry mismatch. REF: point_id={}, new_entry={}. GOT: point_id={}, new_entry={}.",
                            other_point_id, other_new_entry, point_id, new_entry,
                        );
                        false
                    }
                } else {
                    println!(
                        "ERROR! Graph new entry mismatch. No changes or wrong type. REF: point_id={}, new_entry={}",
                        point_id, new_entry);
                    false
                }
            } else {
                println!(
                    "ERROR! Graph new entry no level. No changes or wrong type. REF: point_id={}, new_entry={}",
                    point_id, new_entry);
                false
            }
        } else {
            true
        }
    }
}
