//! This is an empty stub-crate to allow adding sub-crates to the `common` directory
//! and using wildcards in the workspace root.
//!
//! "Real" `common` crate is located at `lib/common/common`!
//!
//! E.g.:
//! ```toml
//! [workspace]
//! members = ["lib/*", "lib/common/*"]
//! ```
//!
//! Without this stub-crate, `lib/*` wildcard would fail, because it requires
//! `lib/common/Cargo.toml` to exist.
//!
//! In a similar vein `lib/common/*` wildcard would fail, if there's `lib/common/src`
//! directory without `Cargo.toml` file, so we have to explicitly put `lib.rs`
//! into the root of the stub-crate.
