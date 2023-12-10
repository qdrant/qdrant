use std::any::Any;

pub type Payload = dyn Any + Send + 'static;

/// Downcast panic payload into a string.
///
/// Downcast `&'static str` and `String` panic payloads into a `&str`.
#[allow(clippy::borrowed_box)]
// We *have to* use `&Box<dyn Any>`, because `Box<dyn Any>` implements `Any` itself,
// so `downcast_str(&boxed_any)` would *not* do an auto-deref to inner `dyn Any`,
// but *coerce* `&Box<dyn Any>` itself to `&dyn Any` :(
pub fn downcast_str(any: &Box<Payload>) -> Option<&str> {
    if let Some(str) = any.downcast_ref::<&'static str>() {
        return Some(str);
    }

    if let Some(str) = any.downcast_ref::<String>() {
        return Some(str);
    }

    None
}
