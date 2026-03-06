use crate::index::field_index::FieldIndex;
use crate::types::PayloadFieldSchema;

pub enum BuildFieldIndexResult {
    /// Index was not built, as operation version is lower than segment version
    SkippedByVersion,
    /// Index was already built
    AlreadyExists,
    /// Incompatible schema
    IncompatibleSchema,
    /// Index was built
    Built {
        indexes: Vec<FieldIndex>,
        schema: PayloadFieldSchema,
    },
}
