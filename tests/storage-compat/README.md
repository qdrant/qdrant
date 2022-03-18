# storage compatibility test

In order to detect quickly breakage of storage compatibility, we make sure any PRs results in a system that is still able to read a snapshot of a previous storage folder.

The data is created by running `/tests/basic_api_test.sh` and saved compressed for reference.

- compression scheme: `tar -cjvf storage.tar.bz2 storage/`

To not burden the git repository with tracking the storage file, it is hosted using [Git Large File Storage](https://git-lfs.github.com/).