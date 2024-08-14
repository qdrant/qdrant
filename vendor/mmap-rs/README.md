# mmap-rs

[![CI](https://github.com/StephanvanSchaik/mmap-rs/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/StephanvanSchaik/mmap-rs/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/mmap-rs.svg)](https://crates.io/crates/mmap-rs)
[![Docs](https://docs.rs/mmap-rs/badge.svg)](https://docs.rs/mmap-rs)

A cross-platform and safe Rust API to create and manage memory mappings in the virtual address space of the calling process.
This crate can be used to create both file mappings and anonymous mappings.
In addition, this crate supports the use of features such as huge pages, locking physical memory, etc. on platforms where those features are available.
Furthermore, this crate allows you to enumerate the memory mappings of a process.

## Changelog

[View CHANGELOG](./CHANGELOG.md).

## Rust version requirements (MSRV)

mmap-rs supports **rustc version 1.67 or greater** since version 0.6.1.

## Supported Platforms

Tier 1 (builds and tests are run in CI):

 * `x86_64-pc-windows-msvc`
 * `x86_64-unknown-linux-gnu`
 * `i686-unknown-linux-gnu`
 * `aarch64-unknown-linux-gnu`
 * `armv7a-unknown-linux-gnueabihf`
 * `x86_64-apple-darwin`
 * `x86_64-unknown-freebsd`

Tier 2 (builds are run in CI):

 * `i686-pc-windows-msvc`
 * `aarch64-pc-windows-msvc`
 * `aarch64-linux-android`
 * `armv7-linux-androideabi`
 * `x86_64-linux-android`
 * `i686-linux-android`
 * `aarch64-apple-ios`

Tier 3 (no CI, but should work):

 * `aarch64-apple-darwin`
 * `x86_64-apple-ios`
 * `i686-unknown-freebsd`

## Features

- [x] Anonymous memory maps.
- [x] File-backed memory maps (`unsafe` - see documentation for details).
- [x] Copy-on-write vs. shared memory maps.
- [x] Inaccessible memory maps (using `PROT_NONE` and `PAGE_NOACCESS`).
- [x] Read-only memory maps.
- [x] Read-write memory maps.
- [x] Executable memory maps.
- [x] RWX memory maps for JIT purposes (`unsafe` - see documentation for details).
- [x] Portable instruction cache flushing.
- [x] Synchronous and asynchronous flushing.
- [x] Support for locking physical memory.
- [x] Huge page support.
- [x] Stack support (also known as `MAP_STACK` on Unix).
- [x] Support to exclude memory maps from core dumps (on Unix only).
- [x] Reserve memory mappings, rather than directly committing them.
- [x] Split and merge memory mappings.
- [x] Query the memory areas of the current/a given process (for a given address or address range).
