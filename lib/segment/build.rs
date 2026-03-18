use std::env;

fn main() {
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")
        .expect("CARGO_CFG_TARGET_ARCH env-var is not defined or is not UTF-8");

    // TODO: Is `CARGO_CFG_TARGET_FEATURE` *always* defined?
    //
    // Cargo docs says that "boolean configurations are present if they are set,
    // and not present otherwise", so, what about "target features"?
    //
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html (Ctrl-F CARGO_CFG_<cfg>)
    let target_feature = env::var("CARGO_CFG_TARGET_FEATURE")
        .expect("CARGO_CFG_TARGET_FEATURE env-var is not defined or is not UTF-8");

    if target_arch == "aarch64" && target_feature.split(',').any(|feat| feat == "neon") {
        let mut builder = cc::Build::new();
        builder.file("src/spaces/metric_f16/cpp/neon.c");
        builder.flag("-O3");
        builder.flag("-march=armv8.2-a+fp16");
        builder.compile("simd_utils");
    }
}
