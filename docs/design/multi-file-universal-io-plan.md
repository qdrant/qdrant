# Plan: Multi-source universal I/O interface

## Problem

Current `UniversalRead` / `UniversalWrite` abstractions assume **one logical storage = one file**. Each instance is opened from a single path and all operations (`read`, `read_batch`, `write`, `write_batch`) apply to that single file.

Storages that are split into **multiple sources** (e.g. gridstore: tracker + N pages; ChunkedVectors: N chunk files; or in the future S3 objects, etc.) cannot express a single batched I/O request that spans sources. As a result:

- **Gridstore**: To serve e.g. 100 `get_value` calls we do many separate reads — from the tracker and from various page files. An io_uring backend could submit one batch of (fd, offset, len) across all involved files, but today we have no interface to describe "these ranges from source A, these from source B".
- **ChunkedVectors**: `for_each_in_batch` calls `get_many_impl` per key; each resolves to one chunk. There is no way to collect (source_id, range) for the whole batch and hand them to a single `read_batch`-like API.

So the limitation is: **batching is only within one source**. We need an abstraction that allows **batch reads (and eventually writes) across multiple sources** so that backends (e.g. io_uring) can implement one submit for the whole batch. The abstraction should not assume "files" only — sources might be files, S3 objects, or similar.

---

## Direction: New trait on top of UniversalRead/UniversalWrite

Introduce a **separate trait** that represents a **multi-source logical storage** and can perform batched I/O across its sources. Keep `UniversalRead`/`UniversalWrite` unchanged for single-file use.

### 1. Naming

- Use **MultiUniversalRead** (not MultiFileRead) so it is clear the abstraction is backend-agnostic (files, S3 objects, etc.).
- Similarly **MultiUniversalWrite** when we add multi-source writes.
- **SourceId** (not FileId) for the opaque identifier of a source in the multi-source storage.

### 2. Core types (in `common/universal_io`)

- **SourceId**  
  - Opaque identifier for "which source in this storage".  
  - Type: either a newtype wrapper (e.g. `u32` or `usize`) or an associated type of the multi-source trait.  
  - Each multi-source storage defines its own mapping (e.g. gridstore: 0 = tracker, 1.. = pages; ChunkedVectors: chunk index).

- **MultiUniversalReadRequest** (or similar)  
  - One read request in a batch: `(SourceId, ElementsRange)`.  
  - Callback-based batch API similar to current `read_batch`: e.g.  
    `read_batch_multi(requests: impl IntoIterator<Item = (SourceId, ElementsRange)>, callback: FnMut(usize, &[T]))`.

- **MultiUniversalRead<T>** (trait)  
  - Represents a logical storage made of multiple sources, each addressable by `SourceId`.  
  - Required capabilities:  
    - Map `SourceId` → underlying handle (conceptually; implementation may hold a collection of `S: UniversalRead<T>>` or equivalent).  
    - **`read_batch_multi<const SEQUENTIAL: bool>(&self, requests, callback) -> Result<()>`**  
      - Takes a batch of `(SourceId, ElementsRange)` and invokes `callback(index, slice)` for each range (order consistent with `requests`).  
  - Optional:  
    - `source_len(&self, source_id: SourceId) -> Result<u64>` if needed by callers.  
    - `populate(&self)` / `clear_ram_cache(&self)` that delegate to all constituent sources (or to the backend's multi-source logic).

- **MultiUniversalWrite<T>** (trait, later)  
  - Same idea for writes: batch of `(SourceId, ElementOffset, &[T])` (or similar).  
  - Can be introduced after the read path is in place.

### 3. Attaching more sources

Some storages grow over time (e.g. gridstore adds a new page). The interface and its minimal implementation should support **attaching additional sources** after creation.

- The **trait** need not require this (some backends might be fixed at construction).  
- The **minimal implementation** (see below) shall support it: e.g. **`attach(&mut self, source: S) -> SourceId`** that adds a new `UniversalRead<T>` to the collection and returns its `SourceId`.  
- So when gridstore creates a new page later, it can attach that new page to the same multi-source view and use the returned `SourceId` in future batch reads.  
- Design detail: `SourceId` can be the index in the underlying `Vec` (or similar), so attaching yields `SourceId(current_len)` and appending is straightforward.

### 4. Relationship to existing traits

- **Do not change** `UniversalRead` / `UniversalWrite` signatures.  
  - Single-source code (e.g. `ImmutableDenseVectors`, single `Page`, single `Tracker`) continues to use them as today.

- **Multi-source trait is "on top"**:  
  - A type that implements `MultiUniversalRead` typically holds multiple `S: UniversalRead<T>>` (or one backend that can do multi-source I/O).  
  - Default implementation for "multiple UniversalRead handles" can be a thin wrapper: loop over requests, dispatch by `SourceId` to the right handle, call existing `read`/`read_batch` per source.  
  - A future io_uring-based backend would implement `MultiUniversalRead` (and possibly `MultiUniversalWrite`) and submit one batched operation for the whole iterator.

- **No requirement** that every `UniversalRead` implement multi-source batching.  
  - Only storages that are logically "multi-source" need to present a `MultiUniversalRead` view when we want to use batched multi-source I/O.

### 5. Scope of this plan: interface + minimal mmap implementation

- **In scope:**  
  - Define the **MultiUniversalRead** trait and related types (**SourceId**, batch request shape) in `common/universal_io`.  
  - Provide a **minimal implementation** backed by mmap (i.e. a type that holds `Vec<S: UniversalRead<T>>` with `S = MmapUniversal<T>` or generic `S`), including:  
    - `read_batch_multi` implemented by dispatching per `(SourceId, range)` to the corresponding `UniversalRead`.  
    - **`attach(&mut self, source: S) -> SourceId`** to add more sources after creation.

- **Out of scope for this plan:**  
  - Changes in **gridstore** (adapter over tracker + pages, refactoring get_value/iteration to use `read_batch_multi`). Defer to a follow-up.  
  - Changes in **ChunkedVectors**. Defer to a follow-up.  
  - io_uring or other backends; those come later.

So we focus only on the interface and its minimal implementation with mmap; no call-site changes in gridstore or elsewhere yet.

### 6. Design alternatives considered

- **Extend UniversalRead with an optional multi-source method**  
  - e.g. `read_batch_multi` with default that returns "not supported".  
  - Con: single-source backends would still need to implement the method; API of UniversalRead becomes more complex.  
  - Pro: one trait. Prefer keeping a separate trait so single-source abstraction stays simple.

- **UniversalRead as "multi-source from the start"**  
  - Every `UniversalRead` would take (SourceId, Range) and we'd have one source per "storage".  
  - Con: large refactor; single-source call sites would have to pass a trivial SourceId everywhere.  
  - Pro: single abstraction. Rejected for refactor cost and ergonomics.

- **Iterator / streaming instead of callback**  
  - `read_batch_multi` could return an iterator of `(usize, Cow<[T]>)` instead of a callback.  
  - Callback matches existing `read_batch` and allows backend to control lifetime of buffers (important for io_uring). We can keep callback and add iterator adapters if needed.

### 7. Implementation order (when implementing)

1. **common/universal_io**  
   - Add **SourceId** (or associated type), **MultiUniversalReadRequest** (or inline `(SourceId, ElementsRange)`), trait **MultiUniversalRead<T>**.  
   - Add **minimal implementation** (e.g. a struct holding `Vec<S: UniversalRead<T>>`) that:  
     - Implements **MultiUniversalRead<T>** with `read_batch_multi` dispatching per (SourceId, range) to the corresponding `S`.  
     - Supports **`attach(&mut self, source: S) -> SourceId`** to add sources and return their id.  
   - Use **MmapUniversal** as the concrete `S` for this minimal impl (or make the struct generic over `S` and test with `MmapUniversal`).

2. **Later (not in this plan)**  
   - Gridstore adapter and refactor to use `read_batch_multi`.  
   - ChunkedVectors integration.  
   - io_uring backend; **MultiUniversalWrite**; other call sites.

---

## Summary

- Add a **multi-source read** abstraction (**MultiUniversalRead<T>**) in parallel to `UniversalRead`/`UniversalWrite`, with a batch API that takes `(SourceId, ElementsRange)` and a callback. Naming avoids "file" so it fits files, S3 objects, etc.
- Support **attaching more sources** in the minimal implementation (e.g. `attach(&mut self, source: S) -> SourceId`) so storages that grow (e.g. new gridstore page) can add sources without rebuilding the view.
- Keep **UniversalRead/UniversalWrite** as the single-source abstraction; do not change their API.
- Use **SourceId** to identify which source in the multi-source storage.
- **Scope:** define the interface and provide a **minimal implementation with mmap** only; do not plan gridstore or other call-site changes yet. Gridstore/ChunkedVectors/io_uring come in follow-ups.

No code changes in this step — this document is the plan only.
