use std::collections::HashMap;

use serde_json::Value;
use tempfile::NamedTempFile;

use super::Sink;
use crate::ambient::AmbientContext;
use crate::ambient::hw::HardwareData;

/// Run `f` in a traced root span.
pub(crate) fn trace(f: impl FnOnce()) -> Vec<Value> {
    run(f).0
}

/// Run `f` measured into a traced root. Return the trace and hwdata.
pub(crate) fn run(f: impl FnOnce()) -> (Vec<Value>, HardwareData) {
    let (_file, sink) = file_sink();
    let ctx = AmbientContext::root("root", None, Some(sink.clone()));
    ctx.measure(f);
    let hw = ctx.hw_data();
    drop(ctx);
    sink.stop();
    (lines(&sink), hw)
}

pub(crate) fn file_sink() -> (NamedTempFile, Sink) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let sink = Sink::file(file.path().to_owned()).unwrap();
    sink.start().unwrap();
    (file, sink)
}

/// The events written into `sink`.
pub(crate) fn lines(sink: &Sink) -> Vec<Value> {
    let mut ids = HashMap::from([(0, 0)]);
    fs_err::read_to_string(sink.current_path())
        .unwrap()
        .lines()
        .map(|line| {
            let mut event: Value = serde_json::from_str(line).unwrap();
            for (key, value) in event.as_object_mut().unwrap() {
                match key.as_str() {
                    "id" | "parent" => {
                        let next = ids.len() as u64;
                        *value = Value::from(*ids.entry(value.as_u64().unwrap()).or_insert(next));
                    }
                    "timestamp" | "started" | "ended" => *value = Value::from("*"),
                    _ => {}
                }
            }
            event
        })
        .collect()
}
