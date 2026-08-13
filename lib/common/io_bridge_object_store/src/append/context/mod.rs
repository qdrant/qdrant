//! The append strategies: one object per strategy, each implementing its
//! own append logic, combined by [`AppendContext`].

mod compose;
mod native;
pub(super) mod part_copy;
mod signed;
#[cfg(test)]
pub(super) mod stub;

pub use compose::ComposeAppend;
pub use native::NativeAppend;
pub use part_copy::PartCopyAppend;
pub use signed::SignedRequestContext;

/// Text of the first `<tag>…</tag>` element in an S3 XML response, with
/// surrounding whitespace trimmed. Tolerates attributes on the opening tag
/// (`<Error xmlns="…">`) and pretty-printed bodies. Deliberately not a
/// general XML parser — S3 error/result elements are flat text.
fn extract_xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open_prefix = format!("<{tag}");
    let close = format!("</{tag}>");

    let mut rest = body;
    let content = loop {
        let after_name = &rest[rest.find(&open_prefix)? + open_prefix.len()..];
        // The tag name must end right here — with the closing `>` or with
        // whitespace-separated attributes — otherwise this occurrence is a
        // longer tag name (e.g. `<CodeDetail>` when looking for `<Code>`).
        if let Some(content) = after_name.strip_prefix('>') {
            break content;
        }
        if after_name.starts_with(char::is_whitespace) {
            break &after_name[after_name.find('>')? + 1..];
        }
        rest = after_name;
    };

    Some(content[..content.find(&close)?].trim())
}

#[cfg(test)]
mod tests {
    use super::extract_xml_tag;

    #[test]
    fn extract_xml_tag_tolerates_formatting_variations() {
        // The plain shape AWS produces.
        assert_eq!(
            extract_xml_tag("<Error><Code>NoSuchKey</Code></Error>", "Code"),
            Some("NoSuchKey"),
        );
        // Attributes on the opening tag.
        assert_eq!(
            extract_xml_tag(r#"<Code xmlns="http://ns">NoSuchKey</Code>"#, "Code"),
            Some("NoSuchKey"),
        );
        // Pretty-printed body: surrounding whitespace is trimmed.
        assert_eq!(
            extract_xml_tag("<Code>\n    NoSuchKey\n</Code>", "Code"),
            Some("NoSuchKey"),
        );
        // A longer tag name sharing the prefix must not match.
        assert_eq!(
            extract_xml_tag("<CodeDetail>X</CodeDetail><Code>Y</Code>", "Code"),
            Some("Y"),
        );
        assert_eq!(extract_xml_tag("<CodeDetail>X</CodeDetail>", "Code"), None);
        // Unclosed or absent elements.
        assert_eq!(extract_xml_tag("<Code>NoSuchKey", "Code"), None);
        assert_eq!(extract_xml_tag("no xml at all", "Code"), None);
    }
}

/// The append strategy of the configured store: one object per strategy,
/// each implementing its own append logic. The backend picks the variant
/// from its config (see [`BlobBackend::append_context`]).
///
/// [`BlobBackend::append_context`]: crate::BlobBackend::append_context
#[derive(Debug, Clone)]
pub enum AppendContext {
    /// The store honors native single-request write-offset appends
    /// (`PutObject` + `x-amz-write-offset-bytes`): S3 Express One Zone,
    /// MinIO AiStor.
    Native(NativeAppend),
    /// Plain S3: appends land as whole-object multipart rewrites whose
    /// prefix parts are server-side `UploadPartCopy` requests.
    PartCopy(PartCopyAppend),
    /// GCS: appends land as server-side `compose` requests — the new data
    /// uploaded as a temporary object, then composed onto the existing one.
    Compose(ComposeAppend),
}
