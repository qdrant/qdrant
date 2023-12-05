use std::any::Any;

type PanicPayload = Box<dyn Any + Send + 'static>;

/// Convert a panic payload into a string
///
/// This converts `String` and `&str` panic payloads into a string.
/// Other payload types are formatted as is, and may be non descriptive.
pub fn panic_payload_into_string(payload: PanicPayload) -> String {
    payload
        .downcast::<&str>()
        .map(|msg| msg.to_string())
        .or_else(|payload| payload.downcast::<String>().map(|msg| msg.to_string()))
        .unwrap_or_else(|payload| format!("{payload:?}"))
}
