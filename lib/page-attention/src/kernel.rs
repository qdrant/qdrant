use std::sync::OnceLock;

/// Which implementation of the kernels this process uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Scalar,
    /// AVX2 + FMA: `vpmaddubsw` + `vpmaddwd` instead of `vpdpbusd`.
    Avx2,
    /// AVX-512F + BW + VL + VNNI (+ F16C for the fp16 pass).
    Avx512,
}

impl Path {
    /// Largest absolute value of an int8 query weight this path can take.
    ///
    /// `vpdpbusd` accumulates in i32 and cannot overflow, so AVX-512 uses the full int8 range.
    /// The AVX2 fallback's `vpmaddubsw` sums TWO `u8 * i8` products into an **i16**, which
    /// saturates above 32767: with the biased levels reaching 255 the weights must stay
    /// within `32767 / (2 * 255) = 64`, so that path quantises the query to 6 bits + sign.
    /// (`night_e2`: an int8 query is free at -0.02..-0.18 pp; one bit less is inside the same
    /// noise, and the exact rescoring pass sees the final ranking anyway.)
    pub const fn q_max(self) -> f32 {
        match self {
            Path::Avx2 => 63.0,
            _ => 127.0,
        }
    }
    pub const fn name(self) -> &'static str {
        match self {
            Path::Scalar => "scalar",
            Path::Avx2 => "avx2",
            Path::Avx512 => "avx512-vnni",
        }
    }
}

fn detect_path() -> Path {
    let forced = std::env::var("KVSTORE_KERNEL").unwrap_or_default();
    let have512 = cfg!(target_arch = "x86_64")
        && is_x86_feature_detected!("avx512f")
        && is_x86_feature_detected!("avx512bw")
        && is_x86_feature_detected!("avx512vl")
        && is_x86_feature_detected!("avx512vnni")
        && is_x86_feature_detected!("f16c");
    let have2 = cfg!(target_arch = "x86_64")
        && is_x86_feature_detected!("avx2")
        && is_x86_feature_detected!("fma")
        && is_x86_feature_detected!("f16c");
    match forced.as_str() {
        "scalar" => Path::Scalar,
        "avx2" if have2 => Path::Avx2,
        "avx512" if have512 => Path::Avx512,
        _ if have512 => Path::Avx512,
        _ if have2 => Path::Avx2,
        _ => Path::Scalar,
    }
}

/// The kernel implementation this process uses (detected once).
pub fn path() -> Path {
    static P: OnceLock<Path> = OnceLock::new();
    *P.get_or_init(detect_path)
}

/// Every path this CPU can run (the tests cross-check them).
pub fn available_paths() -> Vec<Path> {
    let mut v = vec![Path::Scalar];
    let saved = std::env::var("KVSTORE_KERNEL").ok();
    // detection is cheap and side-effect free; probe with the env var out of the way
    std::env::remove_var("KVSTORE_KERNEL");
    let best = detect_path();
    if let Some(s) = saved {
        std::env::set_var("KVSTORE_KERNEL", s);
    }
    if best == Path::Avx512 {
        v.push(Path::Avx2);
        v.push(Path::Avx512);
    } else if best == Path::Avx2 {
        v.push(Path::Avx2);
    }
    v
}
