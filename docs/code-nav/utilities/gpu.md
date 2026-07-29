# GPU utilities (Vulkan / HNSW)

## Overview

Optional GPU acceleration uses **Vulkan** via the [`ash`](https://crates.io/crates/ash) bindings. The main consumer in-tree is **HNSW index graph construction**: batches of points can be linked on the GPU instead of purely on the CPU. The `lib/gpu` crate is **feature-gated**: when the `gpu` feature is disabled, the crate compiles as an empty stub (see `#![cfg(feature = "gpu")]` on the crate root).

## Location

| Area | Path |
|------|------|
| Vulkan helpers (instance, device, buffers, pipelines, shaders) | `lib/gpu/` |
| HNSW GPU builder (batched points, level build, links upload/download) | `lib/segment/src/index/hnsw_index/gpu/` |

## GPU Crate

The `gpu` crate exposes modules re-exported from `lib.rs`: `allocation_callbacks`, `context`, `debug_messenger`, `descriptor_set`, `descriptor_set_layout`, `buffer`, `device`, `instance`, `pipeline`, `shader`.

- **`Instance`**: Vulkan instance setup (with optional debug messenger in tests via `GPU_TEST_INSTANCE` when `testing` is enabled).
- **`Device`**: Wraps a Vulkan `ash::Device`, tracks the parent `Instance`, uses `gpu-allocator` for GPU memory, exposes compute (and transfer) queues, subgroup size, max workgroup counts, max buffer size, half-precision support, and a chosen queue index.
- **`Context`**: Records commands and submits work on a compute queue. Holds command pool, command buffer, fence, and a list of `Arc<dyn Resource>` to keep GPU resources alive for the duration of execution. **Not thread-safe**; ordering is not guaranteed unless callers use `wait_finish` between dependent submissions.
- **`Buffer`**, **`Pipeline`**, **`DescriptorSet`**, **`Shader`**: Supporting types for compute workloads (see respective modules).
- **`Resource`**: Trait (`Send + Sync`) for types whose lifetime must extend through GPU use.

### Error handling

`GpuError` covers allocation failures (`OutOfMemory` for device or host), mapping/recording issues (`OutOfBounds`), missing capabilities (`NotSupported`), synchronization (`NotReady`, `Timeout`), and a catch-all `Other(String)`. `GpuResult<T>` is `Result<T, GpuError>`. Conversions exist from `gpu_allocator::AllocationError` and `vk::Result` so Vulkan return codes map into this enum.

## HNSW GPU Integration

The `gpu` submodule under `hnsw_index` wires segment indexing to the `gpu` crate:

- Submodules include `batched_points`, `gpu_devices_manager`, `gpu_graph_builder`, `gpu_insert_context`, `gpu_level_builder`, `gpu_links`, `gpu_vector_storage`, `gpu_visited_flags`, and `shader_builder`.
- A process-wide `GPU_DEVICES_MANAGER` (`RwLock<Option<GpuDevicesMaganer>>`) coordinates devices.
- Constants such as `GPU_TIMEOUT`, `GPU_GROUPS_COUNT` (default parallelism / “warps”), and `GPU_FORCE_HALF_PRECISION` tune behavior.

`build_hnsw_on_gpu` (in `gpu_graph_builder.rs`) builds an HNSW graph by:

1. Batching point IDs by level (`BatchedPoints`) so each batch maps to a shader invocation.
2. Creating a `GraphLayersBuilder` aligned with the reference graph’s `m`, `ef_construct`, and entry points.
3. Linking an initial prefix of points on the **CPU** (for correctness / bootstrap).
4. Initializing `GpuInsertContext`, then iterating **levels from top to bottom**: upload links, run `build_level_on_gpu`, download links back into the builder.

Vector data and visited flags are handled via `GpuVectorStorage` and related modules; shaders are built through `shader_builder`.

## Feature Gating

- **`lib/gpu/Cargo.toml`**: The package feature `gpu` enables optional dependencies `ash`, `gpu-allocator`, and `shaderc`. Without it, those deps are not linked.
- **`lib/gpu/src/lib.rs`**: The entire crate body is behind `#![cfg(feature = "gpu")]`.
- **`lib/segment/Cargo.toml`**: `gpu = ["gpu/gpu"]` forwards the feature to the `gpu` dependency path crate so segment can compile GPU HNSW code only when requested. The `testing` feature also references `gpu/testing` for shared test instances.

Downstream binaries or libraries enable `segment/gpu` (and thus `gpu/gpu`) when they want Vulkan-backed HNSW construction.

## Cross-References

- HNSW CPU path: `lib/segment/src/index/hnsw_index/` (e.g. `graph_layers_builder`).
- Segment types for HNSW config: `segment::types::HnswConfig`.
- For code navigation of the wider indexing stack, see other docs under `docs/code-nav/`.
