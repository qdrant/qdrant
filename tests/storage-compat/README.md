# Storage compatibility test

In order to detect quickly breakage of storage compatibility, we check that the current code understands the storage format from the latest stable release.

To not burden the git repository with tracking the large binary files, we are pushing storage archives to our [GCP bucket](https://storage.googleapis.com/qdrant-backward-compatibility/).

As features are added, the storage format may change. This means updating the reference storage data and snapshot. This is done by running the `gen_storage_compat_data.sh` script.

## Regenerate storage data

Follow those steps to recreate the reference storage data and snapshot.

1. run `./tests/storage-compat/gen_storage_compat_data.sh`
2. make sure to pick the right version when asked for which system generated the files
3. push the new archives to the GCP bucket (ask for the credentials if you don't have them)
