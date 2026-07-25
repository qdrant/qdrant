//! Minimal S3-compatible object-store client.
//!
//! Only the two operations a read-only shard open needs are implemented, both as plain HTTP GETs:
//! `ListObjectsV2` to discover the objects under a prefix, and a whole-object read for each.
//!
//! The client is *blocking*, which is the whole reason this crate targets `wasm32-wasip2` rather
//! than the browser: WASI gives the guest synchronous sockets, so the same code runs natively and
//! under a WASI runtime with no async runtime, no threads, and no sync/async bridge.
//!
//! Requests are unsigned, so the bucket must allow anonymous reads. TLS is not enabled — `ureq`'s
//! default features are off, so this speaks `http://` only. Presigned URLs and a TLS backend would
//! slot in here without touching anything downstream.

use std::io::Read as _;

/// An object listed under a prefix.
pub struct ListedObject {
    /// Full key, as stored in the bucket.
    pub key: String,
    /// Object size in bytes, per the listing.
    pub size: u64,
}

/// Fetch `url` and return the response body.
pub fn get(url: &str) -> Result<Vec<u8>, String> {
    let mut response = ureq::get(url)
        .call()
        .map_err(|err| format!("fetching {url}: {err}"))?;

    let mut body = Vec::new();
    response
        .body_mut()
        .as_reader()
        .read_to_end(&mut body)
        .map_err(|err| format!("reading {url}: {err}"))?;

    Ok(body)
}

/// List every object under `prefix` in the bucket rooted at `base_url`, following continuation
/// tokens until the listing is exhausted.
///
/// `base_url` is the bucket endpoint (e.g. `http://localhost:9000/my-bucket`), `prefix` the key
/// prefix to list under (e.g. `collection/0`).
pub fn list(base_url: &str, prefix: &str) -> Result<Vec<ListedObject>, String> {
    let base_url = base_url.trim_end_matches('/');
    let mut objects = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut url = format!("{base_url}/?list-type=2&prefix={}", encode(prefix));
        if let Some(token) = &continuation {
            url.push_str(&format!("&continuation-token={}", encode(token)));
        }

        let body = get(&url)?;
        let body = String::from_utf8(body)
            .map_err(|err| format!("listing {prefix}: response was not UTF-8: {err}"))?;

        for contents in tags(&body, "Contents") {
            let Some(key) = tags(contents, "Key").next() else {
                continue;
            };
            let size = tags(contents, "Size")
                .next()
                .and_then(|size| size.trim().parse().ok())
                .unwrap_or(0);

            objects.push(ListedObject {
                key: unescape(key),
                size,
            });
        }

        let truncated = tags(&body, "IsTruncated").next() == Some("true");
        continuation = truncated
            .then(|| tags(&body, "NextContinuationToken").next().map(unescape))
            .flatten();

        if continuation.is_none() {
            break;
        }
    }

    Ok(objects)
}

/// Iterate the text content of every `<tag>…</tag>` in `xml`.
///
/// A real XML parser would be overkill: the `ListObjectsV2` response is a flat, machine-generated
/// document, and the only fields read here (`Key`, `Size`, continuation token) never nest.
fn tags<'a>(xml: &'a str, tag: &'a str) -> impl Iterator<Item = &'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;

    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let end = rest[start..].find(&close)? + start;
        let value = &rest[start..end];
        rest = &rest[end + close.len()..];
        Some(value)
    })
}

/// Undo the XML entity escaping S3 applies to key names.
fn unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Percent-encode a query-string value.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated>
        <Contents><Key>collection/0/segments_manifest.json</Key><Size>412</Size></Contents>
        <Contents><Key>collection/0/a&amp;b.dat</Key><Size>7</Size></Contents>
        </ListBucketResult>"#;

    #[test]
    fn parses_keys_and_sizes_from_a_listing() {
        let keys: Vec<_> = tags(LISTING, "Contents")
            .map(|contents| {
                let key = unescape(tags(contents, "Key").next().unwrap());
                let size: u64 = tags(contents, "Size").next().unwrap().parse().unwrap();
                (key, size)
            })
            .collect();

        assert_eq!(
            keys,
            vec![
                ("collection/0/segments_manifest.json".to_string(), 412),
                ("collection/0/a&b.dat".to_string(), 7),
            ]
        );
    }

    #[test]
    fn encodes_query_values() {
        assert_eq!(encode("collection/0"), "collection/0");
        assert_eq!(encode("a b&c"), "a%20b%26c");
    }
}
