# tracing-forest
[![github-img]][github-url] [![crates-img]][crates-url] [![docs-img]][docs-url]

[github-url]: https://github.com/QnnOkabayashi/tracing-forest
[crates-url]: https://crates.io/crates/tracing-forest
[docs-url]: https://docs.rs/tracing-forest/latest/tracing_forest/
[github-img]: https://img.shields.io/badge/github-8da0cb?style=for-the-badge&labelColor=555555&logo=github
[crates-img]: https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&labelColor=555555&logo=rust
[docs-img]: https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&labelColor=555555&logoColor=white&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K

Preserve contextual coherence among trace data from concurrent tasks.

# Overview

[`tracing`] is a framework for instrumenting programs to collect structured
and async-aware diagnostics via the `Subscriber` trait. The
[`tracing-subscriber`] crate provides tools for composing `Subscriber`s
from smaller units. This crate extends [`tracing-subscriber`] by providing
`ForestLayer`, a `Layer` that preserves contextual coherence of trace
data from concurrent tasks when logging.

This crate is intended for programs running many nontrivial and disjoint
tasks concurrently, like server backends. Unlike other `Subscriber`s
which simply keep track of the context of an event, `tracing-forest` preserves
the contextual coherence when writing logs even in parallel contexts, allowing
readers to easily trace a sequence of events from the same task.

`tracing-forest` is intended for authoring applications.

[`tracing`]: https://crates.io/crates/tracing
[`tracing-subscriber`]: https://crates.io/crates/tracing-subscriber

# Getting started

The easiest way to get started is to enable all features. Do this by
adding the following to your `Cargo.toml` file:
```toml
tracing-forest = { version = "0.1.6", features = ["full"] }
```
Then, add `tracing_forest::init` to your main function:
```rust
fn main() {
    tracing_forest::init();
    // ...
}
```

# Contextual coherence in action

Similar to this crate, the `tracing-tree` crate collects and writes trace
data as a tree. Unlike this crate, it doesn't maintain contextual coherence
in parallel contexts.

Observe the below program, which simulates serving multiple clients concurrently.
```rust
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use tracing_tree::HierarchicalLayer;

#[tracing::instrument]
async fn conn(id: u32) {
    for i in 0..3 {
        some_expensive_operation().await;
        info!(id, "step {}", i);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // Use a `tracing-tree` subscriber
    Registry::default()
        .with(HierarchicalLayer::default())
        .init();

    let connections: Vec<_> = (0..3)
        .map(|id| tokio::spawn(conn(id)))
        .collect();

    for conn in connections {
        conn.await.unwrap();
    }
}
```
`tracing-tree` isn't intended for concurrent use, and this is demonstrated
by the output of the program:
```log
conn id=2
conn id=0
conn id=1
  23ms  INFO step 0, id=2
  84ms  INFO step 0, id=1
  94ms  INFO step 1, id=2
  118ms  INFO step 0, id=0
  130ms  INFO step 1, id=1
  193ms  INFO step 2, id=2

  217ms  INFO step 1, id=0
  301ms  INFO step 2, id=1

  326ms  INFO step 2, id=0

```
We can instead use `tracing-forest` as a drop-in replacement for `tracing-tree`.
```rust
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use tracing_forest::ForestLayer;

#[tracing::instrument]
async fn conn(id: u32) {
    // -- snip --
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // Use a `tracing-forest` subscriber
    Registry::default()
        .with(ForestLayer::default())
        .init();

    // -- snip --
}
```
Now we can easily trace what happened:
```log
INFO     conn [ 150µs | 100.00% ] id: 1
INFO     ┝━ ｉ [info]: step 0 | id: 1
INFO     ┝━ ｉ [info]: step 1 | id: 1
INFO     ┕━ ｉ [info]: step 2 | id: 1
INFO     conn [ 343µs | 100.00% ] id: 0
INFO     ┝━ ｉ [info]: step 0 | id: 0
INFO     ┝━ ｉ [info]: step 1 | id: 0
INFO     ┕━ ｉ [info]: step 2 | id: 0
INFO     conn [ 233µs | 100.00% ] id: 2
INFO     ┝━ ｉ [info]: step 0 | id: 2
INFO     ┝━ ｉ [info]: step 1 | id: 2
INFO     ┕━ ｉ [info]: step 2 | id: 2
```

## License
`tracing-forest` is open-source software, distributed under the MIT license.
