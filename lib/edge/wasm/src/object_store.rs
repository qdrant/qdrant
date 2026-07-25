//! Minimal S3-compatible object-store client built on the browser's `fetch`.
//!
//! Only the two operations a read-only shard open needs are implemented, both as plain HTTP GETs:
//! `ListObjectsV2` to discover the objects under a prefix, and a whole-object read for each. That
//! is deliberately the entire surface — anything richer (range reads, retries, signing) belongs on
//! the async side of the fetch boundary, and the shard itself never gets to see it: by the time
//! [`ReadOnlyEdgeShard`](edge::ReadOnlyEdgeShard) is opened, every byte is already in
//! [`MemFs`](crate::mem_fs::MemFs).
//!
//! Requests are unsigned, so the bucket must allow anonymous reads (and, from a browser, must send
//! permissive CORS headers). Presigned URLs would slot in the same way.

use js_sys::{ArrayBuffer, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, Response};

/// An object listed under a prefix.
pub struct ListedObject {
    /// Full key, as stored in the bucket.
    pub key: String,
    /// Object size in bytes, per the listing.
    pub size: u64,
}

/// Fetch `url` and return the response body, or an error carrying the HTTP status.
pub async fn get(url: &str) -> Result<Vec<u8>, String> {
    let opts = RequestInit::new();
    opts.set_method("GET");

    let request = Request::new_with_str_and_init(url, &opts)
        .map_err(|err| format!("building request for {url}: {}", describe(&err)))?;

    let response: Response = JsFuture::from(global_fetch(&request)?)
        .await
        .map_err(|err| format!("fetching {url}: {}", describe(&err)))?
        .dyn_into()
        .map_err(|_| format!("fetching {url}: response was not a Response"))?;

    if !response.ok() {
        return Err(format!(
            "fetching {url}: HTTP {} {}",
            response.status(),
            response.status_text()
        ));
    }

    let buffer: ArrayBuffer = JsFuture::from(
        response
            .array_buffer()
            .map_err(|err| format!("reading {url}: {}", describe(&err)))?,
    )
    .await
    .map_err(|err| format!("reading {url}: {}", describe(&err)))?
    .dyn_into()
    .map_err(|_| format!("reading {url}: body was not an ArrayBuffer"))?;

    Ok(Uint8Array::new(&buffer).to_vec())
}

/// List every object under `prefix` in the bucket rooted at `base_url`, following continuation
/// tokens until the listing is exhausted.
///
/// `base_url` is the bucket endpoint (e.g. `https://s3.example.com/my-bucket`), `prefix` the key
/// prefix to list under (e.g. `collection/0`).
pub async fn list(base_url: &str, prefix: &str) -> Result<Vec<ListedObject>, String> {
    let base_url = base_url.trim_end_matches('/');
    let mut objects = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut url = format!("{base_url}/?list-type=2&prefix={}", encode(prefix));
        if let Some(token) = &continuation {
            url.push_str(&format!("&continuation-token={}", encode(token)));
        }

        let body = get(&url).await?;
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

/// `fetch` lives on `window` in a page and on the global scope in a worker; reach it through the
/// global object so the module works in both.
fn global_fetch(request: &Request) -> Result<js_sys::Promise, String> {
    let global = js_sys::global();
    let fetch = js_sys::Reflect::get(&global, &JsValue::from_str("fetch"))
        .map_err(|_| "no global `fetch` available".to_string())?;
    let fetch: js_sys::Function = fetch
        .dyn_into()
        .map_err(|_| "global `fetch` is not callable".to_string())?;

    fetch
        .call1(&global, request)
        .map_err(|err| format!("calling fetch: {}", describe(&err)))?
        .dyn_into()
        .map_err(|_| "fetch did not return a Promise".to_string())
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

/// Best-effort human-readable form of a JS exception.
fn describe(err: &JsValue) -> String {
    err.as_string()
        .or_else(|| {
            js_sys::Reflect::get(err, &JsValue::from_str("message"))
                .ok()
                .and_then(|message| message.as_string())
        })
        .unwrap_or_else(|| format!("{err:?}"))
}
