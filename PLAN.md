# Phase 2: Convert GLSL Shaders to Native Slang Syntax

## Overview
Convert all 22 `.comp` shader files from GLSL syntax (compiled with `-lang glsl`) to native Slang syntax. Keep `#include` and preprocessor macros for configuration — full module system migration is Phase 3.

## 1. Update compiler flags in `instance.rs`
- Remove `-lang glsl` flag (Slang native is the default for `.slang` files)
- Remove `-profile glsl_450` (not needed in native Slang mode)
- Remove `-stage compute` (entry point is marked with `[shader("compute")]`)
- Keep `-force-glsl-scalar-layout`, `-emit-spirv-directly`, `-O2`, `-I`, `-entry main`

## 2. Convert `extensions.comp`
- Remove all `#extension` directives (Slang auto-infers capabilities)
- File becomes empty or just a comment

## 3. Convert `common.comp`
- Add `static uint3 gl_GlobalInvocationID;` global (set by entry points)
- Replace `gl_SubgroupInvocationID` refs with `WaveGetLaneIndex()`
- Replace macro: `#define SUBGROUP_ID (gl_GlobalInvocationID.x / SUBGROUP_SIZE)`

## 4. Mechanical find-and-replace across ALL shader files

### Types
| GLSL | Slang |
|------|-------|
| `vec4` | `float4` |
| `vec3` | `float3` |
| `vec2` | `float2` |
| `uvec4` | `uint4` |
| `uvec2` | `uint2` |
| `ivec4` | `int4` |
| `f16vec4` | `half4` |
| `u8vec4` | `vector<uint8_t, 4>` |

### Subgroup → Wave intrinsics
| GLSL | Slang |
|------|-------|
| `subgroupAdd(x)` | `WaveActiveSum(x)` |
| `subgroupMax(x)` | `WaveActiveMax(x)` |
| `subgroupMin(x)` | `WaveActiveMin(x)` |
| `subgroupElect()` | `WaveIsFirstLane()` |
| `subgroupAny(x)` | `WaveActiveAnyTrue(x)` |
| `subgroupAll(x)` | `WaveActiveAllTrue(x)` |
| `subgroupShuffle(v, l)` | `WaveReadLaneAt(v, l)` |
| `subgroupBroadcast(v, l)` | `WaveReadLaneAt(v, l)` |
| `subgroupBallot(x)` | `WaveActiveBallot(x)` |
| `subgroupBallotFindLSB(b)` | Custom `ballotFindLSB(b)` helper |
| `gl_SubgroupInvocationID` | `WaveGetLaneIndex()` |

### Barriers
Since workgroups have exactly 1 subgroup (local_size_x = SUBGROUP_SIZE), subgroup barriers ≡ workgroup barriers:
| GLSL | Slang |
|------|-------|
| `barrier()` | `GroupMemoryBarrierWithGroupSync()` |
| `groupMemoryBarrier()` | `GroupMemoryBarrier()` |
| `memoryBarrier()` | `AllMemoryBarrier()` |
| `subgroupMemoryBarrierShared()` | `GroupMemoryBarrier()` |
| `subgroupMemoryBarrierBuffer()` | `DeviceMemoryBarrier()` |
| `subgroupMemoryBarrier()` | `AllMemoryBarrier()` |

### Shared memory
| GLSL | Slang |
|------|-------|
| `shared ` | `groupshared ` |

### Misc
| GLSL | Slang |
|------|-------|
| `bitCount(x)` | `countbits(x)` |

## 5. Buffer declaration conversion (per-file, manual)

Each GLSL buffer declaration converts to Slang descriptor syntax:

```glsl
// GLSL
layout(set = X, binding = Y) buffer Name { type data[]; } name;
// → Slang
[[vk::binding(Y, X)]] RWStructuredBuffer<type> name;
// Access: name.data[i] → name[i]

// GLSL
layout(set = X, binding = Y) readonly buffer Name { type data[]; } name;
// → Slang
[[vk::binding(Y, X)]] StructuredBuffer<type> name;

// GLSL
layout(set = X, binding = Y) readonly uniform Name { uint m; } name;
// → Slang
struct NameStruct { uint m; };
[[vk::binding(Y, X)]] ConstantBuffer<NameStruct> name;
// Access stays: name.m
```

Files with buffer declarations to convert:
- `links.comp` (1 uniform + 1 storage)
- `visited_flags.comp` (1 uniform + 2 storage)
- `vector_storage.comp` (1 storage, conditional)
- `vector_storage_dense.comp` (4 storage buffers via macros)
- `vector_storage_sq.comp` (1 storage)
- `vector_storage_pq.comp` (2 storage)
- `vector_storage_bq.comp` (1 storage)
- `run_greedy_search.comp` (2 storage + layout in)
- `run_insert_vector.comp` (3 storage + layout in)
- All 5 test shaders (various)

## 6. Entry point conversion (7 main/test shader files)

Remove `#version 450` and `layout(local_size_x=...) in;`.
Add Slang entry point attributes:

```slang
[shader("compute")]
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;  // set the global for include files
    // ... rest of main
}
```

Files: `run_greedy_search.comp`, `run_insert_vector.comp`, and all 5 test shaders.

## 7. Atomics fix in `run_insert_vector.comp`
- Remove the `spirv_asm`-based `slangAtomicExchange` workaround
- Use native Slang: `InterlockedExchange(atomics[other_id], 1u, old_val)`
- This should work in native Slang mode (the crash was a GLSL-compat-mode bug)

## 8. Conversion order
1. Update all 22 shader files (mechanical + manual)
2. Update `instance.rs` compiler flags
3. Run all 56 GPU tests
4. Fix any issues

## Risks
- `vector<uint8_t, 4>` may not be fully supported — may need `uint8_t4` or alternative
- `half4` may need `float16_t` or `half` depending on Slang's type system
- `InterlockedExchange` on `RWStructuredBuffer` may still have issues in native mode — `spirv_asm` fallback ready
- `WaveReadLaneAt` may require dynamically uniform lane index — `WaveShuffle` as alternative
- `subgroupBallotFindLSB` needs manual implementation via `firstbitlow()`
