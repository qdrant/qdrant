# TLS test

## Regenerate certificates

1. run `PROJECT_ROOT/tests/e2e_tests/test_data/gen.sh`
2. replace old certificates in `PROJECT_ROOT/tests/e2e_tests/test_data/cert` with the newly generated ones

# Data compatibility test

In order to detect quickly breakage of storage compatibility, we check that the current code understands the storage format from the latest stable release.

To not burden the git repository with tracking the large binary files, we are pushing storage archives to our [GCP bucket](https://storage.googleapis.com/qdrant-backward-compatibility/).

As features are added, the storage format may change. This means updating the reference storage data and snapshot. This is done by running the [gen_storage_compat_data.sh](test_data/compatibility/gen_storage_compat_data.sh) script.

## Regenerate storage data

Follow those steps to recreate the reference storage data and snapshot.

1. run `PROJECT_ROOT/tests/e2e_tests/test_data/compatibility/gen_storage_compat_data.sh`
2. make sure to pick the right version when asked for which system generated the files
3. push the new archives to the GCP bucket (ask for the credentials if you don't have them)

# Local buildx cache

When no pre-built `qdrant/qdrant:e2e-tests` exists locally, the `qdrant_image` fixture builds one
via a dedicated `qdrant-e2e-builder` (docker-container driver) with a persistent cache at `~/.cache/qdrant-e2e-buildx`.

- First run is cold; subsequent runs reuse the `cargo chef cook` layer while `Cargo.lock` is unchanged.
- Cache is shared across checkouts and worktrees.
- Reset: `rm -rf ~/.cache/qdrant-e2e-buildx`. Remove builder: `docker buildx rm qdrant-e2e-builder`.
- CI never builds here — it loads the image from the `build-e2e-image` artifact.
