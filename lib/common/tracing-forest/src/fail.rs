use crate::cfg_uuid;

pub const SPAN_NOT_IN_CONTEXT: &str = "Span not in context, this is a bug";
pub const OPENED_SPAN_NOT_IN_EXTENSIONS: &str =
    "Span extension doesn't contain `OpenedSpan`, this is a bug";
pub const PROCESSING_ERROR: &str = "Processing logs failed";

cfg_uuid! {
    pub const NO_CURRENT_SPAN: &str = "The subscriber isn't in any spans";
    pub const NO_FOREST_LAYER: &str = "The span has no `Span` in extensions, perhaps you forgot to add a `ForestLayer` to your subscriber?";

    #[cold]
    #[inline(never)]
    pub fn subscriber_not_found<'a, S>() -> &'a S {
        panic!(
            "Subscriber could not be downcasted to `{}`",
            std::any::type_name::<S>()
        );
    }
}
