use std::collections::VecDeque;
use std::sync::Mutex;

use crate::types::PointOffsetType;

pub struct Recorder<T> {
    pub items: VecDeque<T>,
    pub check: bool,
}

impl<T: Eq + std::fmt::Debug> Recorder<T> {
    fn new() -> Self {
        Recorder {
            items: Default::default(),
            check: false,
        }
    }

    fn write(&mut self, item: T) {
        if self.check {
            let check_item = self.items.pop_front().unwrap();
            assert_eq!(check_item, item);
        } else {
            self.items.push_back(item);
        }
    }

    fn check(&mut self, check: bool) {
        self.check = check;
    }
}

type RecorderType = (PointOffsetType, Vec<PointOffsetType>);

lazy_static! {
    static ref RECORDER: Mutex<Recorder<RecorderType>> = Mutex::new(Recorder::new());
}

pub fn record(item: RecorderType) {
    RECORDER.lock().unwrap().write(item);
}

pub fn recorder_check(check: bool) {
    RECORDER.lock().unwrap().check(check);
}

pub fn record_finish() {
    assert!(RECORDER.lock().unwrap().items.is_empty());
}
