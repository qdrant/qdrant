use std::any;

/// Downcast panic payload into a string
///
/// Downcast `&'static str` and `String` panic payloads into a `&str`
pub fn downcast_str(any: &dyn any::Any) -> Option<&str> {
    if let Some(str) = any.downcast_ref::<&'static str>() {
        return Some(str);
    }

    if let Some(str) = any.downcast_ref::<String>() {
        return Some(str);
    }

    None
}
