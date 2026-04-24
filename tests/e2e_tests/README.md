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

# Refresh test durations

E2E tests are split across shards in CI using [pytest-split](https://github.com/jerry-git/pytest-split), which reads `tests/e2e_tests/.test_durations` to balance per-test wall time across shards. The file drifts over time as tests are added, removed, or change in duration; refresh it when shard imbalance becomes noticeable.

1. On a branch, temporarily edit `.github/workflows/integration-tests.yml` — in the `e2e-tests` job, add `--store-durations` to the pytest command and add the upload step right after it:

   ```yaml
   - name: Run e2e tests (shard ${{ matrix.shard }}/3)
     run: |
       uv --project tests run pytest \
         -n 2 \
         --splits 3 --group ${{ matrix.shard }} \
         --store-durations --durations-path tests/e2e_tests/.test_durations \
         tests/e2e_tests -v --tb=short --durations=5 -rs -m "not longrunning"
     shell: bash
     timeout-minutes: 15
   - name: Upload test durations
     if: success()
     uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
     with:
       name: test-durations-shard-${{ matrix.shard }}
       path: tests/e2e_tests/.test_durations
       retention-days: 1
       include-hidden-files: true
   ```

2. Push and let CI run all three shards to completion.
3. Download and merge the per-shard artifacts into one file:

   ```bash
   gh run download <run-id> -p 'test-durations-shard-*'
   python3 -c '
   import json, glob
   merged = {}
   for p in sorted(glob.glob("test-durations-shard-*/.test_durations")):
       with open(p) as f:
           merged.update(json.load(f))
   json.dump(dict(sorted(merged.items())), open("tests/e2e_tests/.test_durations","w"), indent=2)
   print(f"{len(merged)} tests")
   '
   rm -rf test-durations-shard-*
   ```

4. Revert the workflow changes from step 1.
5. Commit the updated `tests/e2e_tests/.test_durations` together with the workflow revert.
