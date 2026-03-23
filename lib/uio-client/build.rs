use std::path::Path;

fn main() -> std::io::Result<()> {
    let source = Path::new("../api/src/grpc/proto/storage_read_service.proto");
    let local = "proto/storage_read_service.proto";

    println!("cargo:rerun-if-changed={local}");

    // When building inside the qdrant workspace, verify that the local copy
    // is in sync with the source of truth in the api crate.
    // When built from crates.io the source path won't exist — just use the local copy.
    if source.exists() {
        println!("cargo:rerun-if-changed={}", source.display());

        let source_contents = std::fs::read_to_string(source)?;
        let local_contents = std::fs::read_to_string(local)?;

        assert_eq!(
            source_contents, local_contents,
            "proto out of sync: local `{local}` differs from `{}`. \
             Update the local copy to match the api crate.",
            source.display(),
        );
    }

    tonic_build::configure()
        .out_dir("src/generated/")
        .compile(&[local], &["proto"])?;

    Ok(())
}
