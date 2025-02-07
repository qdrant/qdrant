use crate::types::SeqNumberType;

/// This version type ensures that the `internal_to_version` vector can safely grow without introducing synthetic versions
///
/// The only special thing about this is that SeqNumberType::MAX means no version. Make sure to always use [VersionType::none()] as default when resizing.
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(transparent)]
pub struct VersionType(SeqNumberType);

impl VersionType {
    pub const fn none() -> Self {
        Self(SeqNumberType::MAX)
    }
}

impl From<SeqNumberType> for VersionType {
    fn from(version: SeqNumberType) -> Self {
        Self(version)
    }
}

// This is the important conversion to distinguish between None and Some.
impl From<VersionType> for Option<SeqNumberType> {
    fn from(version: VersionType) -> Self {
        match version.0 {
            SeqNumberType::MAX => None,
            v => Some(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_type() {
        assert_eq!(VersionType::from(SeqNumberType::MAX), VersionType::none());
        assert_eq!(Option::from(VersionType::from(42)), Some(42));

        assert_eq!(Option::<SeqNumberType>::from(VersionType::none()), None);
        assert_eq!(Option::from(VersionType(42)), Some(42));

        assert_eq!(size_of::<VersionType>(), size_of::<SeqNumberType>());
    }
}
