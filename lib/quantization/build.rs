use std::env;

fn main() {
    println!("cargo:rerun-if-changed=cpp");
    let mut builder = cc::Build::new();

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")
        .expect("CARGO_CFG_TARGET_ARCH env-var is not defined or is not UTF-8");

    // `CARGO_CFG_TARGET_FEATURE` is *not* always defined: Cargo omits `CARGO_CFG_<cfg>`
    // when the cfg has no values, and some targets enable no target features at all.
    // `rustc --target powerpc64le-unknown-linux-gnu --print cfg` (same for `s390x-unknown-linux-gnu`)
    // prints no `target_feature` line, so the variable is absent and must be read as "no features".
    //
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html (Ctrl-F CARGO_CFG_<cfg>)
    let target_feature = match env::var("CARGO_CFG_TARGET_FEATURE") {
        Ok(target_feature) => target_feature,
        Err(env::VarError::NotPresent) => String::new(),
        Err(err @ env::VarError::NotUnicode(_)) => {
            panic!("CARGO_CFG_TARGET_FEATURE env-var is not UTF-8: {err}")
        }
    };

    if target_arch == "x86_64" {
        builder.file("cpp/sse.c");
        builder.file("cpp/avx2.c");

        if builder.get_compiler().is_like_msvc() {
            // Only one `/arch:` is effective (last wins). `/arch:AVX2` enables AVX2 and
            // lower ISAs (haswell-equivalent), matching `-march=haswell` below.
            // Passing `/arch:SSE`/`SSE2` after `/arch:AVX2` downgrades the ISA and breaks
            // clang-cl (e.g. cargo-xwin), which requires target features for intrinsics.
            builder.flag("/arch:AVX2");
        } else {
            builder.flag("-march=haswell");
        }

        // O3 optimization level
        builder.flag("-O3");
        // Use popcnt instruction
        builder.flag("-mpopcnt");
    } else if target_arch == "aarch64" && target_feature.split(',').any(|feat| feat == "neon") {
        builder.file("cpp/neon.c");
        builder.flag("-O3");
    } else {
        // No SIMD sources for this target; nothing to compile.
        return;
    }

    builder.compile("simd_utils");
}
