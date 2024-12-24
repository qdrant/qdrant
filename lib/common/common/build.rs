fn main() {
    // Required for tango benchmarks, see:
    // https://github.com/bazhenov/tango/blob/v0.6.0/README.md#getting-started
    println!("cargo:rustc-link-arg-benches=-rdynamic");
    println!("cargo:rerun-if-changed=build.rs");
}
