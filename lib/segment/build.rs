use std::env;
use std::io::Write;
use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/spaces/metric_f16/cpp/neon.c");
    let mut builder = cc::Build::new();

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
        builder.file("src/spaces/metric_f16/cpp/neon.c");
        builder.flag("-O3");

        let c_code = r#"
        #include <stdio.h>
        #ifdef __ARM_FEATURE_FP16_VECTOR_ARITHMETIC
        int main() {
            return 0; // FP16 supported
        }
        #else
        int main() {
            return 1; // FP16 not supported
        }
        #endif
        "#;

        let mut child = Command::new("cc")
            .args(["-x", "c", "-"])
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to execute process");

        {
            let stdin = child.stdin.as_mut().expect("Failed to open stdin");
            stdin
                .write_all(c_code.as_bytes())
                .expect("Failed to write to stdin");
        }

        let output = child.wait().expect("failed to wait on child");

        if output.success() {
            println!("cargo:rustc-cfg=feature=\"neon_fp16\"");
            println!("cargo:warning=NEON FP16 support detected. Enabling feature `neon_fp16`.");
            builder.compile("simd_utils");
        } else {
            println!(
                "cargo:warning=NEON FP16 support not detected. Disabling feature `neon_fp16`."
            );
        }
    }
}
