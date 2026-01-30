# Qdrant Edge Examples

This directory contains examples demonstrating how to use the Qdrant Edge library.

## Try It Out

To run the examples from within this repository, use the following command:

```bash
cargo run -p edge --example demo
```

If you want to run these examples in your own project, add the following dependencies to your `Cargo.toml`:

```toml
[dependencies]
edge = { git = "https://github.com/qdrant/qdrant.git", branch = "dev", package = "edge" }
segment = { git = "https://github.com/qdrant/qdrant.git", branch = "dev", package = "segment" }
shard = { git = "https://github.com/qdrant/qdrant.git", branch = "dev", package = "shard" }
uuid = { version = "1", features = ["v4"] }
serde_json = "1"
tempfile = "3"
ordered-float = "5"
fs-err = "3"
```
