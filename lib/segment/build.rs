use std::env;

fn main() {
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

    if target_arch == "aarch64" && target_feature.split(',').any(|feat| feat == "neon") {
        let mut builder = cc::Build::new();
        builder.file("src/spaces/metric_f16/cpp/neon.c");
        builder.flag("-O3");
        builder.flag("-march=armv8.2-a+fp16");
        builder.compile("simd_utils");
    }
}
