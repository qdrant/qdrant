//! Format selectors for serialized graph links.
//!
//! [`GraphLinksFormat`] names a serialization format; [`GraphLinksFormatParam`]
//! is its "input" counterpart that additionally carries the vector accessor
//! required by the `CompressedWithVectors` format.

use super::vectors::GraphLinksVectors;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GraphLinksFormat {
    Plain,
    Compressed,
    CompressedWithVectors,
}

/// Similar to [`GraphLinksFormat`], won't let you use `CompressedWithVectors`
/// without providing the vectors.
#[derive(Clone, Copy)]
pub enum GraphLinksFormatParam<'a> {
    Plain,
    Compressed,
    CompressedWithVectors(&'a dyn GraphLinksVectors),
}

impl GraphLinksFormat {
    /// Create the corresponding [`GraphLinksFormatParam`].
    ///
    /// # Panics
    ///
    /// Panics if `CompressedWithVectors` is selected, but `vectors` is `None`.
    #[cfg(test)]
    pub fn with_param_for_tests<'a, Q: GraphLinksVectors>(
        &self,
        vectors: Option<&'a Q>,
    ) -> GraphLinksFormatParam<'a> {
        match self {
            GraphLinksFormat::Plain => GraphLinksFormatParam::Plain,
            GraphLinksFormat::Compressed => GraphLinksFormatParam::Compressed,
            GraphLinksFormat::CompressedWithVectors => match vectors {
                Some(v) => GraphLinksFormatParam::CompressedWithVectors(v),
                None => panic!(),
            },
        }
    }

    pub fn is_with_vectors(&self) -> bool {
        match self {
            GraphLinksFormat::Plain | GraphLinksFormat::Compressed => false,
            GraphLinksFormat::CompressedWithVectors => true,
        }
    }
}

impl<'a> GraphLinksFormatParam<'a> {
    pub fn as_format(&self) -> GraphLinksFormat {
        match self {
            GraphLinksFormatParam::Plain => GraphLinksFormat::Plain,
            GraphLinksFormatParam::Compressed => GraphLinksFormat::Compressed,
            GraphLinksFormatParam::CompressedWithVectors(_) => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }
}
