# Storage compatibility test

In order to detect quickly breakage of storage compatibility, we make sure any PRs results in a system that is still able to read a snapshot of a previous storage folder.

To not burden the git repository with tracking the large binary files, it is using [Git Large File Storage](https://git-lfs.github.com/).

## Regenerate storage data

Follow those steps to recreate the reference storage data.

1 - the application needs to be configured in order to capture the `graph.bin` files for HNSW index, decrease `storage.optimizers.indexing_threshold_kb` to `1`.
2 - start the application `cargo run --bin qdrant`
3 - run `./tests/storage-compat/gen_storage_compat_data.sh`
4 - compare the content of the storage folder with the current `storage.tar.bz2` as a sanity check
5 - compress the storage folder using `tar -cjvf storage.tar.bz2 storage/`.
6 - replace existing `storage.tar.bz2` with `mv storage.tar.bz2 tests/storage-compat/`
7 - run compatibility test with the new archive `./tests/storage-compat/storage-compatibility.sh`
8 - push change
