/// Shader compilation for CUDA/HIP targets.
///
/// Pipeline:
///   1. `slangc shader.slang -target cuda -D USE_BDA -D WAVE_SIZE=N -D SLANG_CUDA_STRUCTURED_BUFFER_NO_COUNT ...`
///      → `/tmp/shader.cu`
///   2. For HIP: `hipcc --genco --amdgpu-target=gfxXXX shader.cu -o shader.co` → binary blob
///      For CUDA: `nvcc -ptx shader.cu -arch sm_XX -o shader.ptx` → text PTX
///   3. Parse the `.cu` source to extract `GlobalParams_0` member order.
///   4. Load module from binary with `cuModuleLoadData` / `hipModuleLoadData`.
use std::collections::HashMap;
use std::path::Path;

use super::device::CudaDevice;
use super::driver::Runtime;
use crate::{GpuError, GpuResult};

/// Compiled shader binary + parameter layout extracted from generated CUDA source.
pub struct CudaShader {
    /// The loaded binary bytes (HIP .co or CUDA PTX).
    pub binary: Vec<u8>,

    /// Ordered list of (set_index=0, binding_index) extracted from GlobalParams_0 in the .cu file.
    /// Each slot corresponds to one 8-byte pointer in SLANG_globalParams.
    pub param_order: Vec<(usize, usize)>,

    /// Block dimensions parsed from `[numthreads(X, Y, Z)]` in the shader source.
    pub block_dims: (u32, u32, u32),
}

static COMPILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Compile a Slang shader to a CUDA/HIP binary.
///
/// `shader` is the Slang source code.
/// `shader_name` is the shader module name (used as file name).
/// `defines` are additional `-D` preprocessor defines.
/// `includes` are virtual include files written to a temp dir.
/// `device` supplies the target architecture and wave size.
pub fn compile_shader(
    shader: &str,
    shader_name: &str,
    defines: Option<&HashMap<String, Option<String>>>,
    includes: Option<&HashMap<String, String>>,
    device: &CudaDevice,
) -> GpuResult<CudaShader> {
    let counter = COMPILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_dir =
        std::env::temp_dir().join(format!("qdrant_cuda_{}_{counter}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| GpuError::Other(format!("Failed to create temp dir: {e}")))?;

    // Write include files.
    if let Some(includes) = includes {
        for (filename, source) in includes {
            std::fs::write(tmp_dir.join(filename), source)
                .map_err(|e| GpuError::Other(format!("Failed to write include {filename}: {e}")))?;
        }
    }

    let result = compile_inner(shader, shader_name, defines, includes, &tmp_dir, device);

    // Keep temp files for debugging.
    if std::env::var("QDRANT_GPU_KEEP_TEMP").is_ok() {
        log::info!("Keeping temp dir: {}", tmp_dir.display());
    } else {
        // Best-effort cleanup.
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    result
}

fn compile_inner(
    shader: &str,
    shader_name: &str,
    defines: Option<&HashMap<String, Option<String>>>,
    includes: Option<&HashMap<String, String>>,
    tmp_dir: &Path,
    device: &CudaDevice,
) -> GpuResult<CudaShader> {
    // Use only the basename so path separators in shader_name don't create missing subdirs.
    let base_name = std::path::Path::new(shader_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(shader_name);
    let module_name = base_name
        .strip_suffix(".slang")
        .or_else(|| base_name.strip_suffix(".comp"))
        .unwrap_or(base_name);

    // Write shader source.
    let slang_path = tmp_dir.join(format!("{module_name}.slang"));
    std::fs::write(&slang_path, shader)
        .map_err(|e| GpuError::Other(format!("Failed to write shader source: {e}")))?;

    let cu_path = tmp_dir.join(format!("{module_name}.cu"));

    // Build slangc command.
    let slangc = slangc_path();
    let mut cmd = std::process::Command::new(&slangc);
    cmd.arg(&slang_path)
        .arg("-target")
        .arg("cuda")
        .arg("-D")
        .arg("USE_BDA")
        .arg("-D")
        .arg("SLANG_CUDA_STRUCTURED_BUFFER_NO_COUNT")
        .arg("-line-directive-mode")
        .arg("none")
        .arg("-I")
        .arg(tmp_dir)
        .arg("-o")
        .arg(&cu_path);

    // Forward caller-provided defines (skip USE_BDA if caller also sets it).
    if let Some(defs) = defines {
        for (k, v) in defs {
            if k == "USE_BDA" {
                continue; // already added above
            }
            if let Some(v) = v {
                cmd.arg("-D").arg(format!("{k}={v}"));
            } else {
                cmd.arg("-D").arg(k);
            }
        }
    }

    let out = cmd
        .output()
        .map_err(|e| GpuError::Other(format!("Failed to run slangc: {e}")))?;
    if !out.status.success() && !cu_path.exists() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(GpuError::Other(format!(
            "slangc failed for {shader_name}:\n{stderr}"
        )));
    }

    let cu_source = std::fs::read_to_string(&cu_path)
        .map_err(|e| GpuError::Other(format!("Failed to read generated .cu file: {e}")))?;

    // Collect all Slang sources (main shader + includes) for binding analysis.
    let mut all_sources: Vec<&str> = vec![shader];
    if let Some(incs) = includes {
        for content in incs.values() {
            all_sources.push(content.as_str());
        }
    }

    // Parse GlobalParams member order from the generated CUDA source.
    let param_order = parse_global_params_order(&cu_source, &all_sources);
    log::debug!("param_order for {shader_name}: {param_order:?}");

    // Parse [numthreads(X, Y, Z)] from the shader source for CUDA block dimensions.
    let block_dims = parse_numthreads(shader, defines, includes, device.subgroup_size() as u32);
    log::debug!("block_dims for {shader_name}: {block_dims:?}");

    // Now compile the .cu to a GPU binary.
    let binary = match device.runtime() {
        Runtime::Hip => compile_hip(tmp_dir, module_name, &cu_path, device)?,
        Runtime::Cuda => compile_nvcc(tmp_dir, module_name, &cu_path, device)?,
    };

    Ok(CudaShader {
        binary,
        param_order,
        block_dims,
    })
}

/// Parse `GlobalParams_0` struct from the generated CUDA source and map each
/// member to the correct `(set_index, binding_index)` by parsing
/// `[[vk::binding(N, S)]]` annotations from the original Slang shader sources.
///
/// Slang generates something like:
/// ```c
/// struct GlobalParams_0 {
///     RWStructuredBuffer<float> scores_0;
///     StructuredBuffer<VectorAddresses_0 > vector_addrs_0;
///     StructuredBuffer<MultivectorOffset_0> multivector_offsets_0;
/// };
/// ```
/// Members can come from multiple descriptor sets (set 0, set 1, etc.).
/// We strip the trailing `_N` suffix Slang adds (e.g., `scores_0` → `scores`)
/// and look up the original name in the binding map built from Slang sources.
pub fn parse_global_params_order(cu_source: &str, shader_sources: &[&str]) -> Vec<(usize, usize)> {
    let members = parse_global_params_members(cu_source);
    if members.is_empty() {
        return Vec::new();
    }

    // Build constant table and binding map from all Slang sources.
    let constants = parse_slang_constants(shader_sources);
    let binding_map = parse_slang_bindings(shader_sources, &constants);

    members
        .iter()
        .map(|member| {
            let original = strip_slang_suffix(member);
            if let Some(&(set, binding)) = binding_map.get(original) {
                (set, binding)
            } else {
                log::warn!(
                    "No [[vk::binding(...)]] found for GlobalParams_0 member \
                     '{member}' (original: '{original}'); defaulting to (0, 0)"
                );
                (0, 0)
            }
        })
        .collect()
}

/// Parse `[numthreads(X, Y, Z)]` from the shader source to determine CUDA block dimensions.
///
/// Values can be integer literals or named constants resolved from defines and includes.
/// Falls back to `(subgroup_size, 1, 1)` if the attribute is not found or cannot be resolved.
fn parse_numthreads(
    shader_source: &str,
    defines: Option<&HashMap<String, Option<String>>>,
    includes: Option<&HashMap<String, String>>,
    subgroup_size: u32,
) -> (u32, u32, u32) {
    let default_dims = (subgroup_size, 1, 1);

    // Find [numthreads(X, Y, Z)] in the shader source.
    let prefix = "[numthreads(";
    let start = match shader_source.find(prefix) {
        Some(s) => s + prefix.len(),
        None => return default_dims,
    };
    let end = match shader_source[start..].find(")]") {
        Some(e) => start + e,
        None => return default_dims,
    };

    let args_str = &shader_source[start..end];
    let parts: Vec<&str> = args_str.split(',').map(|s| s.trim()).collect();
    if parts.len() != 3 {
        return default_dims;
    }

    // Build a resolution map from defines and static const declarations in includes.
    let mut constants: HashMap<&str, u32> = HashMap::new();
    if let Some(defs) = defines {
        for (key, val) in defs {
            if let Some(val_str) = val {
                if let Ok(n) = val_str.parse::<u32>() {
                    constants.insert(key.as_str(), n);
                }
            }
        }
    }
    if let Some(incs) = includes {
        for source in incs.values() {
            for line in source.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("static const uint ") {
                    if let Some(eq_pos) = rest.find('=') {
                        let name = rest[..eq_pos].trim();
                        let val_str = rest[eq_pos + 1..].trim().trim_end_matches(';').trim();
                        if let Ok(n) = val_str.parse::<u32>() {
                            constants.insert(name, n);
                        }
                    }
                }
            }
        }
    }

    let resolve =
        |s: &str| -> Option<u32> { s.parse::<u32>().ok().or_else(|| constants.get(s).copied()) };

    match (resolve(parts[0]), resolve(parts[1]), resolve(parts[2])) {
        (Some(x), Some(y), Some(z)) => (x, y, z),
        _ => default_dims,
    }
}

/// Parse member names from the `GlobalParams_0` struct in generated CUDA source.
fn parse_global_params_members(cu_source: &str) -> Vec<String> {
    let start = match cu_source.find("struct GlobalParams_0") {
        Some(s) => s,
        None => return Vec::new(),
    };
    let body_start = match cu_source[start..].find('{') {
        Some(b) => start + b + 1,
        None => return Vec::new(),
    };
    let body_end = match cu_source[body_start..].find("};") {
        Some(e) => body_start + e,
        None => return Vec::new(),
    };

    let body = &cu_source[body_start..body_end];
    let mut members = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Each line: "TypeName<T > varname_N;" — variable name is the last word.
        let line = line.trim_end_matches(';').trim();
        if let Some(varname) = line.split_whitespace().last() {
            members.push(varname.to_string());
        }
    }
    members
}

/// Parse `static const [type] NAME = VALUE;` from Slang source files.
/// Returns a map of constant name → integer value.
fn parse_slang_constants(sources: &[&str]) -> HashMap<String, usize> {
    let mut constants = HashMap::new();
    for &source in sources {
        for line in source.lines() {
            let line = line.trim();
            // Match "static const uint NAME = VALUE;" or "static const int NAME = VALUE;"
            let rest = if let Some(r) = line.strip_prefix("static const uint ") {
                r
            } else if let Some(r) = line.strip_prefix("static const int ") {
                r
            } else {
                continue;
            };
            // rest = "NAME = VALUE;"
            if let Some(eq_pos) = rest.find('=') {
                let name = rest[..eq_pos].trim().to_string();
                let val_str = rest[eq_pos + 1..].trim().trim_end_matches(';').trim();
                if let Ok(val) = val_str.parse::<usize>() {
                    constants.insert(name, val);
                }
            }
        }
    }
    constants
}

/// Evaluate a binding expression that is either an integer literal or a named constant.
fn eval_binding_expr(s: &str, constants: &HashMap<String, usize>) -> Option<usize> {
    let s = s.trim();
    if let Ok(n) = s.parse::<usize>() {
        Some(n)
    } else {
        constants.get(s).copied()
    }
}

/// Parse `[[vk::binding(N, S)]]` annotations from Slang source files.
/// Returns a map from variable name → (set_index, binding_index).
fn parse_slang_bindings(
    sources: &[&str],
    constants: &HashMap<String, usize>,
) -> HashMap<String, (usize, usize)> {
    let mut binding_map = HashMap::new();
    for &source in sources {
        for line in source.lines() {
            let line = line.trim();
            // Match: [[vk::binding(binding_expr, set_expr)]] Type varname;
            let rest = match line.strip_prefix("[[vk::binding(") {
                Some(r) => r,
                None => continue,
            };
            let paren_close = match rest.find(")]]") {
                Some(p) => p,
                None => continue,
            };
            let args_str = &rest[..paren_close];
            let mut args = args_str.splitn(2, ',');
            let binding_expr = match args.next() {
                Some(s) => s.trim(),
                None => continue,
            };
            let set_expr = match args.next() {
                Some(s) => s.trim(),
                None => continue,
            };
            let binding = match eval_binding_expr(binding_expr, constants) {
                Some(b) => b,
                None => continue,
            };
            let set = match eval_binding_expr(set_expr, constants) {
                Some(s) => s,
                None => continue,
            };

            // Variable name is the last word in the declaration after ")]].
            let decl = rest[paren_close + 3..].trim();
            if let Some(varname) = extract_slang_varname(decl) {
                binding_map.insert(varname, (set, binding));
            }
        }
    }
    binding_map
}

/// Extract the variable name from a declaration like:
/// "RWStructuredBuffer<float> scores;" or "ConstantBuffer<T> param;"
fn extract_slang_varname(decl: &str) -> Option<String> {
    // Strip trailing semicolon, then take the last whitespace-delimited token.
    let decl = decl.trim_end_matches(';').trim();
    // Stop before any array subscript, e.g. "buf[4]" → "buf"
    let decl = decl.split('[').next().unwrap_or(decl).trim();
    decl.split_whitespace().last().map(|s| s.to_string())
}

/// Strip Slang's `_N` numeric suffix from generated CUDA names.
/// E.g., `"scores_0"` → `"scores"`, `"pq_divisions_0"` → `"pq_divisions"`.
fn strip_slang_suffix(name: &str) -> &str {
    if let Some(pos) = name.rfind('_') {
        let suffix = &name[pos + 1..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            return &name[..pos];
        }
    }
    name
}

/// Compile a `.cu` file with `hipcc` to a HIP binary (`.co`).
fn compile_hip(
    tmp_dir: &Path,
    module_name: &str,
    cu_path: &Path,
    device: &CudaDevice,
) -> GpuResult<Vec<u8>> {
    // Post-process the generated CUDA source for HIP compatibility.
    let hip_cu_path = tmp_dir.join(format!("{module_name}_hip.cu"));
    let hip_source = make_hip_compatible(cu_path)?;
    std::fs::write(&hip_cu_path, &hip_source)
        .map_err(|e| GpuError::Other(format!("Failed to write HIP-patched source: {e}")))?;

    let out_path = tmp_dir.join(format!("{module_name}.co"));

    let hipcc = find_hipcc()?;
    let offload_arch = device.amdgpu_target.as_deref().unwrap_or("native");

    let mut cmd = std::process::Command::new(&hipcc);
    cmd.arg("--genco");
    #[cfg(not(target_os = "windows"))]
    cmd.arg("-fPIC");
    cmd.arg("-O3")
        .arg(format!("--offload-arch={offload_arch}"))
        .arg(&hip_cu_path)
        .arg("-o")
        .arg(&out_path);

    let out = cmd
        .output()
        .map_err(|e| GpuError::Other(format!("Failed to run hipcc: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        log::error!(
            "hipcc exited with status {:?}, stderr:\n{stderr}",
            out.status.code()
        );
        if !stdout.is_empty() {
            log::error!("hipcc stdout:\n{stdout}");
        }
        if !out_path.exists() {
            return Err(GpuError::Other(format!("hipcc failed:\n{stderr}")));
        }
        log::warn!("hipcc reported errors but .co file exists; continuing");
    }

    std::fs::read(&out_path)
        .map_err(|e| GpuError::Other(format!("Failed to read hipcc output: {e}")))
}

/// Replace Slang's CUDA prelude include with a HIP-compatible header, and
/// fix `extern "C" __constant__` declarations (HIP linker requires definitions,
/// not just external declarations, for constant symbols in device code objects).
fn make_hip_compatible(cu_path: &Path) -> GpuResult<String> {
    let source = std::fs::read_to_string(cu_path)
        .map_err(|e| GpuError::Other(format!("Failed to read .cu file: {e}")))?;

    // Replace Slang's CUDA-only prelude include with a HIP-compatible one.
    // The prelude include line looks like: #include "...slang-cuda-prelude.h"
    let source = if let Some(prelude_line_end) = source.find('\n') {
        let first_line = &source[..prelude_line_end];
        if first_line.contains("slang-cuda-prelude.h") {
            format!("{HIP_PRELUDE}\n{}", &source[prelude_line_end + 1..])
        } else {
            source
        }
    } else {
        source
    };

    // Remove `extern "C"` from __constant__ declarations.
    // HIP's --genco requires constant memory to be *defined* (not just declared) in the module.
    let source = source.replace("extern \"C\" __constant__", "__constant__");

    Ok(source)
}

/// A HIP-compatible replacement for `slang-cuda-prelude.h`.
///
/// Slang's CUDA prelude uses CUDA-specific intrinsics that aren't available in HIP.
/// This prelude provides all types and functions that Slang-generated CUDA code uses:
///   - Basic typedefs: `ulonglong`, `uchar`, `ushort`, `uint`, `half`, etc.
///   - `RWStructuredBuffer<T>` / `StructuredBuffer<T>` (pointer wrappers)
///   - `FixedArray<T, N>` (fixed-size local arrays)
///   - Wave intrinsics: `_getLaneId`, `_waveSum`, `__ballot_sync`
///   - Vector element accessors: `_slang_vector_get_element`
///   - A `_SlangDimHelper` type that supports `blockIdx * blockDim + threadIdx`
const HIP_PRELUDE: &str = r#"// HIP-compatible replacement for slang-cuda-prelude.h
#pragma once
#include <hip/hip_runtime.h>
#include <hip/hip_fp16.h>

#define SLANG_FORCE_INLINE __forceinline__
#define SLANG_CUDA_CALL __device__
#define SLANG_PRELUDE_EXPORT
#define SLANG_PRELUDE_ASSERT(x)

// Wave size constant (can be overridden with -DSLANG_CUDA_WARP_SIZE=N for 64-wide targets)
#ifndef SLANG_CUDA_WARP_SIZE
#define SLANG_CUDA_WARP_SIZE 32
#endif
#define SLANG_CUDA_WARP_MASK (SLANG_CUDA_WARP_SIZE - 1)

// Basic typedefs (from Slang's CUDA prelude)
typedef unsigned long long ulonglong;
typedef long long longlong;
typedef unsigned char uchar;
typedef unsigned short ushort;
typedef unsigned int uint;
typedef __half half;
typedef __half2 half2;

// Half-precision vector types: HIP provides __half and __half2 natively;
// __half3 and __half4 are generated by Slang but not defined by HIP.
struct __attribute__((aligned(4))) __half3 { __half x, y, z; };
struct __attribute__((aligned(8))) __half4 { __half x, y, z, w; };
typedef __half3 half3;
typedef __half4 half4;

// Constructor functions for half vectors (4-arg and scalar broadcast)
__forceinline__ __device__ __half3 make___half3(__half x, __half y, __half z) { return {x, y, z}; }
__forceinline__ __device__ __half4 make___half4(__half x, __half y, __half z, __half w) { return {x, y, z, w}; }
__forceinline__ __device__ __half3 make___half3(__half x) { return {x, x, x}; }
__forceinline__ __device__ __half4 make___half4(__half x) { return {x, x, x, x}; }

// Slang uses SLANG_INFINITY for float +infinity (e.g. in WaveActiveMax initialization)
#ifndef SLANG_INFINITY
#define SLANG_INFINITY INFINITY
#endif

// With SLANG_CUDA_STRUCTURED_BUFFER_NO_COUNT each buffer is just a T* pointer.
template<typename T>
struct RWStructuredBuffer {
    T* data;
    __device__ __forceinline__ T& operator[](size_t index) const { return data[index]; }
};
template<typename T>
struct StructuredBuffer {
    T* data;
    // Return T& (not const T&) so that &buf[i] yields T* — Slang's slang_ldg_0 expects T*.
    __device__ __forceinline__ T& operator[](size_t index) const { return data[index]; }
};

// Fixed-size array (Slang uses this for local arrays in generated CUDA code)
template<typename T, size_t N>
struct FixedArray {
    T data[N];
    __device__ __forceinline__ const T& operator[](size_t i) const { return data[i]; }
    __device__ __forceinline__ T& operator[](size_t i) { return data[i]; }
};

// WarpMask type (32-bit for 32-wide wavefronts; for 64-wide use unsigned long long)
typedef int WarpMask;

// Lane ID within the current wavefront
__forceinline__ __device__ uint32_t _getLaneId() {
    return threadIdx.x & SLANG_CUDA_WARP_MASK;
}

// __shfl_sync: HIP provides __shfl() without the mask argument; the mask is ignored.
// Slang-generated kernels call __shfl_sync directly for WaveReadLaneAt etc.
template<typename T>
__forceinline__ __device__ T __shfl_sync(uint mask, T val, int srcLane, int width = warpSize) {
    return __shfl(val, srcLane, width);
}

// __ballot_sync: HIP provides __ballot() returning 64-bit; narrowed to 32-bit for RDNA (32-wide).
// Slang-generated kernels call this directly with mask=0xFFFFFFFF.
__forceinline__ __device__ uint __ballot_sync(uint mask, int pred) {
    return (uint)__ballot(pred);
}
__forceinline__ __device__ uint __ballot_sync(uint mask, bool pred) {
    return (uint)__ballot((int)pred);
}

// __any_sync / __all_sync: HIP provides __any() / __all() without the mask argument.
__forceinline__ __device__ int __any_sync(uint mask, int pred) { return __any(pred); }
__forceinline__ __device__ int __any_sync(uint mask, bool pred) { return __any((int)pred); }
__forceinline__ __device__ int __all_sync(uint mask, int pred) { return __all(pred); }
__forceinline__ __device__ int __all_sync(uint mask, bool pred) { return __all((int)pred); }

// Wave sum reduction using __shfl_xor (available in all HIP versions without extra flags)
template<typename T>
__device__ __forceinline__ T _waveSum(WarpMask mask, T val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val += __shfl_xor(val, offset);
    return val;
}
template<>
__device__ __forceinline__ unsigned _waveSum<unsigned>(WarpMask mask, unsigned val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val += (unsigned)__shfl_xor((int)val, offset);
    return val;
}
template<>
__device__ __forceinline__ int _waveSum<int>(WarpMask mask, int val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val += __shfl_xor(val, offset);
    return val;
}

// Scalar-to-vector broadcast constructors (Slang generates make_uint4(x) for splat).
// The 4-arg make_T4(x,y,z,w) versions come from HIP's hip_runtime.h.
#define SLANG_MAKE_VECTOR_FROM_SCALAR(T) \
    __forceinline__ __device__ T##2 make_##T##2(T x) { return make_##T##2(x, x); }         \
    __forceinline__ __device__ T##3 make_##T##3(T x) { return make_##T##3(x, x, x); }      \
    __forceinline__ __device__ T##4 make_##T##4(T x) { return make_##T##4(x, x, x, x); }
SLANG_MAKE_VECTOR_FROM_SCALAR(int)
SLANG_MAKE_VECTOR_FROM_SCALAR(uint)
SLANG_MAKE_VECTOR_FROM_SCALAR(float)
SLANG_MAKE_VECTOR_FROM_SCALAR(short)
SLANG_MAKE_VECTOR_FROM_SCALAR(ushort)
SLANG_MAKE_VECTOR_FROM_SCALAR(char)
SLANG_MAKE_VECTOR_FROM_SCALAR(uchar)

// Bit operations
__forceinline__ __device__ uint U32_countbits(uint v) { return __popc(v); }
__forceinline__ __device__ uint U32_firstbitlow(uint v) { return v == 0 ? ~0u : (uint)(__ffs(v) - 1); }
__forceinline__ __device__ uint U32_firstbithigh(uint v) {
    if ((int)v < 0) v = ~v;
    return v == 0 ? ~0u : 31u - (uint)__clz(v);
}

// Scalar math wrappers (Slang emits these instead of calling CUDA built-ins directly)
__forceinline__ __device__ float  F32_abs(float x)          { return fabsf(x); }
__forceinline__ __device__ float  F32_sqrt(float x)         { return sqrtf(x); }
__forceinline__ __device__ float  F32_ceil(float x)         { return ceilf(x); }
__forceinline__ __device__ float  F32_floor(float x)        { return floorf(x); }
__forceinline__ __device__ float  F32_round(float x)        { return roundf(x); }
__forceinline__ __device__ float  F32_exp(float x)          { return expf(x); }
__forceinline__ __device__ float  F32_log(float x)          { return logf(x); }
__forceinline__ __device__ float  F32_log2(float x)         { return log2f(x); }
__forceinline__ __device__ float  F32_sin(float x)          { return sinf(x); }
__forceinline__ __device__ float  F32_cos(float x)          { return cosf(x); }
__forceinline__ __device__ float  F32_min(float a, float b) { return fminf(a, b); }
__forceinline__ __device__ float  F32_max(float a, float b) { return fmaxf(a, b); }
__forceinline__ __device__ float  F32_pow(float a, float b) { return powf(a, b); }
__forceinline__ __device__ float  F32_fmod(float a, float b){ return fmodf(a, b); }
__forceinline__ __device__ float  F32_truncate(float x)     { return truncf(x); }
__forceinline__ __device__ bool   F32_isnan(float x)        { return isnan(x); }
__forceinline__ __device__ bool   F32_isinf(float x)        { return isinf(x); }
__forceinline__ __device__ bool   F32_isfinite(float x)     { return isfinite(x); }
__forceinline__ __device__ double F64_abs(double x)         { return fabs(x); }
__forceinline__ __device__ double F64_sqrt(double x)        { return sqrt(x); }
__forceinline__ __device__ int    I32_abs(int x)            { return abs(x); }
__forceinline__ __device__ int    I32_min(int a, int b)     { return min(a, b); }
__forceinline__ __device__ int    I32_max(int a, int b)     { return max(a, b); }
__forceinline__ __device__ uint   U32_min(uint a, uint b)   { return min(a, b); }
__forceinline__ __device__ uint   U32_max(uint a, uint b)   { return max(a, b); }

// Wave min/max/or/and reductions
template<typename T>
__device__ __forceinline__ T _waveMin(WarpMask mask, T val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val = min(val, __shfl_xor(val, offset));
    return val;
}
template<typename T>
__device__ __forceinline__ T _waveMax(WarpMask mask, T val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val = max(val, __shfl_xor(val, offset));
    return val;
}
template<typename T>
__device__ __forceinline__ T _waveOr(WarpMask mask, T val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val |= (T)__shfl_xor((int)val, offset);
    return val;
}
template<typename T>
__device__ __forceinline__ T _waveAnd(WarpMask mask, T val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val &= (T)__shfl_xor((int)val, offset);
    return val;
}
template<>
__device__ __forceinline__ float _waveMin<float>(WarpMask mask, float val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val = fminf(val, __shfl_xor(val, offset));
    return val;
}
template<>
__device__ __forceinline__ float _waveMax<float>(WarpMask mask, float val) {
    for (int offset = SLANG_CUDA_WARP_SIZE / 2; offset > 0; offset >>= 1)
        val = fmaxf(val, __shfl_xor(val, offset));
    return val;
}

// Vector element accessors (T2/T3/T4 variants; T1 omitted as HIP doesn't define float1 etc.)
#define SLANG_VEC_GET(T) \
    __forceinline__ __device__ T _slang_vector_get_element(T##2 v, int i) { return (&v.x)[i]; } \
    __forceinline__ __device__ T _slang_vector_get_element(T##3 v, int i) { return (&v.x)[i]; } \
    __forceinline__ __device__ T _slang_vector_get_element(T##4 v, int i) { return (&v.x)[i]; }
SLANG_VEC_GET(float)
SLANG_VEC_GET(int)
SLANG_VEC_GET(uint)
SLANG_VEC_GET(short)
SLANG_VEC_GET(ushort)
SLANG_VEC_GET(char)
SLANG_VEC_GET(uchar)
__forceinline__ __device__ half _slang_vector_get_element(half2 v, int i) {
    return i == 0 ? v.x : v.y;
}
__forceinline__ __device__ __half _slang_vector_get_element(__half3 v, int i) { return (&v.x)[i]; }
__forceinline__ __device__ __half _slang_vector_get_element(__half4 v, int i) { return (&v.x)[i]; }
__forceinline__ __device__ __half* _slang_vector_get_element_ptr(__half3* v, int i) { return &v->x + i; }
__forceinline__ __device__ __half* _slang_vector_get_element_ptr(__half4* v, int i) { return &v->x + i; }

// Vector element pointer accessors
#define SLANG_VEC_GET_PTR(T) \
    __forceinline__ __device__ T* _slang_vector_get_element_ptr(T##2* v, int i) { return &v->x + i; } \
    __forceinline__ __device__ T* _slang_vector_get_element_ptr(T##3* v, int i) { return &v->x + i; } \
    __forceinline__ __device__ T* _slang_vector_get_element_ptr(T##4* v, int i) { return &v->x + i; }
SLANG_VEC_GET_PTR(float)
SLANG_VEC_GET_PTR(int)
SLANG_VEC_GET_PTR(uint)

// HIP builtin dim types don't support operator* / operator+ between each other.
// Define a helper type that Slang's (blockIdx * blockDim + threadIdx).x pattern works with.
// Also provides implicit conversion to uint3 for cases like: uint3 v = blockIdx * blockDim + threadIdx;
struct _SlangDimHelper {
    uint32_t x, y, z;
    __device__ __forceinline__ _SlangDimHelper operator*(const _SlangDimHelper& o) const {
        return {x * o.x, y * o.y, z * o.z};
    }
    __device__ __forceinline__ _SlangDimHelper operator+(const _SlangDimHelper& o) const {
        return {x + o.x, y + o.y, z + o.z};
    }
    __device__ __forceinline__ operator uint3() const { return make_uint3(x, y, z); }
};
__device__ __forceinline__ _SlangDimHelper _slang_blockIdx()  {
    return {(uint32_t)blockIdx.x,  (uint32_t)blockIdx.y,  (uint32_t)blockIdx.z};
}
__device__ __forceinline__ _SlangDimHelper _slang_blockDim()  {
    return {(uint32_t)blockDim.x,  (uint32_t)blockDim.y,  (uint32_t)blockDim.z};
}
__device__ __forceinline__ _SlangDimHelper _slang_threadIdx() {
    return {(uint32_t)threadIdx.x, (uint32_t)threadIdx.y, (uint32_t)threadIdx.z};
}
#undef blockIdx
#undef blockDim
#undef threadIdx
#define blockIdx  (_slang_blockIdx())
#define blockDim  (_slang_blockDim())
#define threadIdx (_slang_threadIdx())
"#;

/// Compile a `.cu` file with `nvcc` to a native CUBIN (`.cubin`).
///
/// CUBIN is pre-compiled native GPU code for the target architecture,
/// avoiding the JIT compilation overhead of PTX and potentially producing
/// better-optimised code.  Falls back to PTX if no compute capability is
/// available (PTX can be JIT-compiled for any GPU).
fn compile_nvcc(
    tmp_dir: &Path,
    module_name: &str,
    cu_path: &Path,
    device: &CudaDevice,
) -> GpuResult<Vec<u8>> {
    // Prefer CUBIN (native) when we know the target architecture.
    let use_cubin = device.compute_capability().is_some();
    let ext = if use_cubin { "cubin" } else { "ptx" };
    let out_path = tmp_dir.join(format!("{module_name}.{ext}"));

    let nvcc = find_nvcc()?;
    let mut cmd = std::process::Command::new(&nvcc);
    cmd.arg(if use_cubin { "-cubin" } else { "-ptx" })
        .arg("-O3")
        .arg("--use_fast_math")
        // Must match the define passed to slangc so the Slang CUDA prelude
        // uses pointer-only StructuredBuffer (no count field).
        .arg("-DSLANG_CUDA_STRUCTURED_BUFFER_NO_COUNT");

    // On Windows, nvcc requires MSVC cl.exe as the host compiler.
    // Point it there via -ccbin if cl.exe isn't already in PATH.
    #[cfg(target_os = "windows")]
    if let Some(ccbin) = find_msvc_ccbin() {
        cmd.arg("-ccbin").arg(&ccbin);
    }

    // Pass target architecture from the device's compute capability.
    if let Some((major, minor)) = device.compute_capability() {
        cmd.arg(format!("-arch=sm_{major}{minor}"));
    }

    // Add Slang include directory so nvcc can find slang-cuda-prelude.h.
    let slangc = slangc_path();
    if let Some(slang_bin) = std::path::Path::new(&slangc).parent() {
        if let Some(slang_dir) = slang_bin.parent() {
            cmd.arg("-I").arg(slang_dir);
        }
    }

    cmd.arg(cu_path).arg("-o").arg(&out_path);

    let out = cmd
        .output()
        .map_err(|e| GpuError::Other(format!("Failed to run nvcc: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        log::error!(
            "nvcc exited with status {:?}, stderr:\n{stderr}",
            out.status.code()
        );
        if !stdout.is_empty() {
            log::error!("nvcc stdout:\n{stdout}");
        }
        if !out_path.exists() {
            let mut msg = format!("nvcc failed:\n{stderr}");
            if !stdout.is_empty() {
                msg.push_str(&format!("\nstdout:\n{stdout}"));
            }
            return Err(GpuError::Other(msg));
        }
        log::warn!("nvcc reported errors but .{ext} file exists; continuing");
    }

    let mut binary = std::fs::read(&out_path)
        .map_err(|e| GpuError::Other(format!("Failed to read nvcc output: {e}")))?;
    if !use_cubin {
        // cuModuleLoadData requires PTX to be null-terminated.
        if binary.last() != Some(&0) {
            binary.push(0);
        }
    }
    Ok(binary)
}

fn find_hipcc() -> GpuResult<String> {
    // Check ROCM_PATH or HIP_PATH env variable first.
    let env_path = std::env::var("ROCM_PATH")
        .ok()
        .or_else(|| std::env::var("HIP_PATH").ok());

    let env_candidates: Vec<String> = if let Some(base) = env_path {
        if cfg!(target_os = "windows") {
            vec![
                format!("{base}\\bin\\hipcc.bin.exe"),
                format!("{base}\\bin\\hipcc.exe"),
            ]
        } else {
            vec![format!("{base}/bin/hipcc")]
        }
    } else {
        #[cfg(not(target_os = "windows"))]
        {
            ["/opt/rocm/bin/hipcc", "/usr/bin/hipcc"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        }
        #[cfg(target_os = "windows")]
        {
            // Search common AMD HIP SDK install paths.
            let mut paths = Vec::new();
            if let Ok(prog_files) = std::env::var("ProgramFiles") {
                for ver in ["6.3", "6.2", "6.1", "6.0"] {
                    paths.push(format!(
                        "{prog_files}\\AMD\\ROCm\\{ver}\\bin\\hipcc.bin.exe"
                    ));
                }
            }
            paths
        }
    };

    for candidate in env_candidates
        .iter()
        .map(String::as_str)
        .chain(["hipcc"].iter().copied())
    {
        if std::path::Path::new(candidate).exists() || which_in_path(candidate) {
            return Ok(candidate.to_string());
        }
    }
    Err(GpuError::NotSupported(
        "hipcc not found; install ROCm/HIP SDK or set ROCM_PATH/HIP_PATH".to_string(),
    ))
}

/// Find the MSVC `cl.exe` directory for use as nvcc's `-ccbin`.
///
/// nvcc on Windows requires MSVC's `cl.exe` as the host compiler. If it's not
/// already in PATH we locate it via `vswhere.exe` or environment variables.
#[cfg(target_os = "windows")]
fn find_msvc_ccbin() -> Option<String> {
    // If cl.exe is already reachable, nvcc will find it on its own.
    if which_in_path("cl") {
        return None;
    }

    // Try VCToolsInstallDir (set inside a VS Developer Command Prompt).
    if let Ok(vc_tools) = std::env::var("VCToolsInstallDir") {
        let dir = std::path::Path::new(&vc_tools)
            .join("bin")
            .join("Hostx64")
            .join("x64");
        if dir.join("cl.exe").exists() {
            return Some(dir.to_string_lossy().into_owned());
        }
    }

    // Use vswhere.exe (ships with every VS 2017+ install at a fixed path).
    let vswhere = r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe";
    if std::path::Path::new(vswhere).exists() {
        if let Ok(output) = std::process::Command::new(vswhere)
            .args([
                "-latest",
                "-products",
                "*",
                "-requires",
                "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                "-property",
                "installationPath",
            ])
            .output()
        {
            let vs_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !vs_path.is_empty() {
                let msvc_dir = std::path::Path::new(&vs_path)
                    .join("VC")
                    .join("Tools")
                    .join("MSVC");
                if let Ok(entries) = std::fs::read_dir(&msvc_dir) {
                    // Pick the latest toolset version.
                    let mut versions: Vec<_> = entries
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                        .collect();
                    versions.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
                    if let Some(latest) = versions.first() {
                        let cl_dir = latest.path().join("bin").join("Hostx64").join("x64");
                        if cl_dir.join("cl.exe").exists() {
                            return Some(cl_dir.to_string_lossy().into_owned());
                        }
                    }
                }
            }
        }
    }

    log::warn!("Could not locate MSVC cl.exe for nvcc -ccbin; nvcc may fail");
    None
}

fn find_nvcc() -> GpuResult<String> {
    // Check CUDA_PATH env variable first (set by NVIDIA installer on both platforms).
    if let Ok(cuda_path) = std::env::var("CUDA_PATH") {
        let nvcc = std::path::Path::new(&cuda_path).join("bin").join("nvcc");
        if nvcc.exists() {
            return Ok(nvcc.to_string_lossy().into_owned());
        }
        let nvcc_exe = nvcc.with_extension("exe");
        if nvcc_exe.exists() {
            return Ok(nvcc_exe.to_string_lossy().into_owned());
        }
    }

    #[cfg(not(target_os = "windows"))]
    let candidates: &[&str] = &["/usr/local/cuda/bin/nvcc"];
    #[cfg(target_os = "windows")]
    let candidates: &[&str] = &[];

    for candidate in candidates {
        if std::path::Path::new(candidate).exists() {
            return Ok(candidate.to_string());
        }
    }
    if which_in_path("nvcc") {
        return Ok("nvcc".to_string());
    }
    Err(GpuError::NotSupported(
        "nvcc not found; install CUDA toolkit or set CUDA_PATH".to_string(),
    ))
}

fn which_in_path(name: &str) -> bool {
    if name.contains('/') || name.contains('\\') {
        return false;
    }
    if let Ok(path_var) = std::env::var("PATH") {
        let separator = if cfg!(target_os = "windows") {
            ';'
        } else {
            ':'
        };
        for dir in path_var.split(separator) {
            let candidate = std::path::Path::new(dir).join(name);
            if candidate.exists() {
                return true;
            }
            if cfg!(target_os = "windows") && !name.ends_with(".exe") {
                if candidate.with_extension("exe").exists() {
                    return true;
                }
            }
        }
    }
    false
}

fn slangc_path() -> String {
    if let Ok(slang_dir) = std::env::var("SLANG_DIR") {
        let path = format!("{slang_dir}/bin/slangc");
        if std::path::Path::new(&path).exists() {
            return path;
        }
        // On Windows, check with .exe extension.
        let path_exe = format!("{path}.exe");
        if std::path::Path::new(&path_exe).exists() {
            return path_exe;
        }
    }
    "slangc".to_string()
}
