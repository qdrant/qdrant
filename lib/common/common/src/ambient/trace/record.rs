use std::time::Instant;

use super::event::Event;
use super::sink::{SINK, elapsed_ns};
use super::span::Context;

/// `println!`-like macro to record a [Event::Mark] in the trace.
#[doc(hidden)]
#[macro_export]
macro_rules! __ambient_mark {
    ($($arg:tt)*) => {
        if $crate::ambient::trace::enabled() {
            $crate::ambient::trace::record_mark(format!($($arg)*));
        }
    };
}
pub use __ambient_mark as mark;

#[doc(hidden)]
pub fn record_mark(text: String) {
    let Some(sink) = SINK.get() else { return };
    sink.send(Event::Mark {
        parent: Context::current().0,
        at_ns: elapsed_ns(sink.origin, Instant::now()),
        text,
    });
}

/// Record [Event::Sections].
pub fn file_sections(path: &str, sections: Vec<(&'static str, u64)>) {
    if let Some(sink) = SINK.get() {
        let path = path.to_owned();
        sink.send(Event::Sections { path, sections });
    }
}
