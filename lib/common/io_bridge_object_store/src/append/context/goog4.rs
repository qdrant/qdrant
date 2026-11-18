//! `GOOG4-HMAC-SHA256` request signing: GCS's own V4 signature for HMAC
//! keys, the AWS SigV4 algorithm with Google's names (`x-goog-*` headers,
//! `goog4_request` scope, `storage` service).
//!
//! The `object_store` signer only produces `AWS4-HMAC-SHA256`, which puts
//! `x-amz-date` / `x-amz-content-sha256` on the request; GCS refuses any
//! request mixing `x-amz-*` with `x-goog-*` headers, so a request that must
//! carry an `x-goog-*` precondition (`x-goog-if-generation-match`) has to be
//! signed with this scheme instead. The same HMAC key works for both.

use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use object_store::aws::AwsCredential;
use sha2::{Digest as _, Sha256};
use url::Url;

const ALGORITHM: &str = "GOOG4-HMAC-SHA256";
const KEY_PREFIX: &str = "GOOG4";
const SERVICE: &str = "storage";
const REQUEST_TYPE: &str = "goog4_request";
pub(super) const DATE_HEADER: &str = "x-goog-date";
pub(super) const CONTENT_SHA256_HEADER: &str = "x-goog-content-sha256";

/// The headers to put on a request so that it carries a valid `GOOG4`
/// signature: `extra_headers`, the date and payload-hash headers, and
/// `authorization`. Every returned header except `authorization` is signed,
/// as is `host` (from `url`), which the HTTP client sets itself.
///
/// `url` must already be in canonical form: its path percent-encoded with
/// [`encode_path_segment`] and its query exactly the canonical query string
/// (e.g. `compose=`). Header names in `extra_headers` must be lowercase.
pub(super) fn sign(
    credential: &AwsCredential,
    region: &str,
    method: &http::Method,
    url: &Url,
    extra_headers: &[(&str, String)],
    body: &[u8],
    now: DateTime<Utc>,
) -> Vec<(String, String)> {
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload_hash = HEXLOWER.encode(&Sha256::digest(body));

    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_string(),
    };

    let mut headers: Vec<(String, String)> = extra_headers
        .iter()
        .map(|(name, value)| (name.to_string(), value.trim().to_string()))
        .collect();
    headers.push((DATE_HEADER.to_string(), timestamp.clone()));
    headers.push((CONTENT_SHA256_HEADER.to_string(), payload_hash.clone()));

    let mut signed: Vec<(String, String)> = headers.clone();
    signed.push(("host".to_string(), host));
    signed.sort();

    let canonical_headers: String = signed
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_headers = signed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        path = url.path(),
        query = url.query().unwrap_or_default(),
    );

    let scope = format!("{date}/{region}/{SERVICE}/{REQUEST_TYPE}");
    let string_to_sign = format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        HEXLOWER.encode(&Sha256::digest(canonical_request.as_bytes())),
    );

    let key = signing_key(
        KEY_PREFIX,
        &credential.secret_key,
        &date,
        region,
        SERVICE,
        REQUEST_TYPE,
    );
    let signature = HEXLOWER.encode(&hmac_sha256(&key, string_to_sign.as_bytes()));

    headers.push((
        "authorization".to_string(),
        format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            credential.key_id,
        ),
    ));
    headers
}

/// The V4 signing key: an HMAC chain over the date, region, service and
/// request type, seeded with `prefix + secret` (`AWS4…` / `GOOG4…`).
fn signing_key(
    prefix: &str,
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    request_type: &str,
) -> [u8; 32] {
    let key = hmac_sha256(format!("{prefix}{secret}").as_bytes(), date.as_bytes());
    let key = hmac_sha256(&key, region.as_bytes());
    let key = hmac_sha256(&key, service.as_bytes());
    hmac_sha256(&key, request_type.as_bytes())
}

/// HMAC-SHA256 (RFC 2104) over [`Sha256`].
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// Percent-encode one path segment the way V4 signing canonicalizes it:
/// everything but the RFC 3986 unreserved characters.
pub(super) fn encode_path_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    /// A conditional compose, signed at a fixed time. The expected
    /// signature comes from an independent signer (openssl, in the
    /// GCS-verified probe script), whose canonical requests matched the
    /// ones GCS reports back byte for byte.
    #[test]
    fn signs_a_conditional_compose() {
        let credential = AwsCredential {
            key_id: "GOOG1EXAMPLE".to_string(),
            secret_key: "secret".to_string(),
            token: None,
        };
        let url =
            Url::parse("https://storage.googleapis.com/bucket/dir/append.dat?compose=").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 11, 17, 9, 30, 0).unwrap();

        let headers = sign(
            &credential,
            "auto",
            &http::Method::PUT,
            &url,
            &[
                ("content-type", "application/xml".to_string()),
                ("x-goog-if-generation-match", "42".to_string()),
            ],
            b"<ComposeRequest/>",
            now,
        );

        let get = |name: &str| {
            headers
                .iter()
                .find(|(header, _)| header == name)
                .map(|(_, value)| value.as_str())
        };
        assert_eq!(get(DATE_HEADER), Some("20261117T093000Z"));
        assert_eq!(
            get(CONTENT_SHA256_HEADER),
            Some("3f56c0b5f358afc825c6dfb284b1666854d12981969e9053151d82e3ebcbe857"),
        );
        assert_eq!(
            get("authorization"),
            Some(
                "GOOG4-HMAC-SHA256 Credential=GOOG1EXAMPLE/20261117/auto/storage/goog4_request, \
                 SignedHeaders=content-type;host;x-goog-content-sha256;x-goog-date;x-goog-if-generation-match, \
                 Signature=1352531b08aac3305686b0f14be31756389e84467784b6fada288cc733ca67b9"
            ),
        );
        assert!(
            headers.iter().all(|(name, _)| !name.starts_with("x-amz-")),
            "GCS rejects x-amz-* next to x-goog-* headers",
        );
    }
}
