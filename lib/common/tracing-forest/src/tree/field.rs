#[cfg(feature = "smallvec")]
pub(crate) type FieldSet = smallvec::SmallVec<[Field; 3]>;
#[cfg(not(feature = "smallvec"))]
pub(crate) type FieldSet = Vec<Field>;

/// A key-value pair recorded from trace data.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Field {
    key: &'static str,
    value: String,
}

impl Field {
    pub(crate) fn new(key: &'static str, value: String) -> Self {
        Field { key, value }
    }

    /// Returns the field's key.
    pub fn key(&self) -> &'static str {
        self.key
    }

    /// Returns the field's value.
    pub fn value(&self) -> &str {
        &self.value
    }
}
