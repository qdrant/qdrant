# Storage compatibility test

In order to detect quickly breakage of storage compatibility, we make sure any PRs results in a system that is still able to read a snapshot of a previous storage folder.

To not burden the git repository with tracking the large binary files, it is using [Git Large File Storage](https://git-lfs.github.com/).

As features are added, the storage format may change and then must be refreshed on `dev`.

The compatibility is ensured by checking that the current code understands the storage format from the `master` branch.

## Regenerate storage data

Follow those steps to recreate the reference storage data and snapshot.

1. run `./tests/storage-compat/gen_storage_compat_data.sh`
2. run compatibility test with the new archive `./tests/storage-compat/storage-compatibility.sh`
3. Git push LFS changes `git lfs push origin $branch`
4. Git push other changes
