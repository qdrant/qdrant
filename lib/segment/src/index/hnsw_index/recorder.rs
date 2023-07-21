use std::{collections::VecDeque, sync::Mutex};

use crate::types::PointOffsetType;

pub struct Recorder<T> {
    pub items: VecDeque<T>,
}

impl<T: Eq + std::fmt::Debug> Recorder<T> {
    fn new() -> Self {
        Recorder { items: Default::default() }
    }

    fn write(&mut self, item: T) {
        self.items.push_back(item);
    }

    fn check(&mut self, item: T) {
        let check_item = self.items.pop_front().unwrap();
        assert_eq!(check_item, item);
    }
}

type RecorderType = (PointOffsetType, Vec<PointOffsetType>);

lazy_static! {
static ref RECORDER: Mutex<Recorder<RecorderType>> = Mutex::new(Recorder::new());
}

pub fn record_write(item: RecorderType) {
    RECORDER.lock().unwrap().write(item);
}

pub fn record_check(item: RecorderType) {
    RECORDER.lock().unwrap().check(item);
}

pub fn record_finish() {
    assert!(RECORDER.lock().unwrap().items.is_empty());
}
