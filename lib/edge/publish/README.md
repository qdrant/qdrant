# Qdrant Edge

Qdrant Edge is a lightweight, in-process vector search engine designed for embedded devices, autonomous systems, and mobile agents. It enables on-device retrieval with minimal memory footprint, no background services, and optional synchronization with Qdrant Cloud.

For connecting to remote Qdrant instances, use the [`qdrant-client`](https://crates.io/crates/qdrant-client) crate instead.

- Website: https://qdrant.tech/edge/
- Documentation: https://qdrant.tech/documentation/edge/
- Examples: https://github.com/qdrant/qdrant/tree/dev/lib/edge/publish/examples

## Process-wide initialization

`EdgeShard::new` and `EdgeShard::load` initialize default feature flags and check
multi-mmap support when these values have not been configured. A failed mmap
check disables multi-mmap; an error running the check also produces a warning.

These settings are shared by all shards in the process and cannot be changed
after initialization. The automatic mmap check uses the first shard's storage
path; other filesystems are not probed.

See the [initialization example](https://github.com/qdrant/qdrant/blob/dev/lib/edge/publish/examples/src/bin/initialization.rs)
for automatic initialization during shard creation and reload.
