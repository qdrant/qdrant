use std::any::Any;

pub type Payload = dyn Any + Send + 'static;

/// Downcast panic payload into a string.
///
/// Downcast `&'static str` and `String` panic payloads into a `&str`.
pub fn downcast_str<'a>(any: &'a Box<Payload>) -> Option<&'a str> {
    if let Some(str) = any.downcast_ref::<&'static str>() {
        return Some(str);
    }

    if let Some(str) = any.downcast_ref::<String>() {
        return Some(str);
    }

    None
}
