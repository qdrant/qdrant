---
name: patch-release
description: Release a Qdrant patch version (vX.Y.Z, Z > 0): cherry-pick dev into master, bump versions, write release notes, tag.
---

# Qdrant patch release

`dev` must be releasable at any moment, so a patch release takes **every** commit merged
into `dev` since the last release, features included. Features ship silently in a patch
and get announced in the next minor release. The only exceptions are commits that
differ between the branches by design, such as version bumps: `dev` carries `-dev`
versions and `master` carries release versions.

Below, `X.Y.Z` is the version being released, `X.Y.(Z+1)` is the next one.

## 1. Sync branches

```bash
git fetch origin --tags
git checkout dev    && git pull --ff-only origin dev
git checkout master && git pull --ff-only origin master
```

## 2. Cherry-pick dev into master

`tools/missed_cherry_picks.sh` lists dev commits not yet on master (oldest first, by
patch-id, so already-picked commits are skipped). Apply them in that order:

```bash
./tools/missed_cherry_picks.sh | cut -d' ' -f1 > /tmp/picks.txt
while read -r sha; do git cherry-pick "$sha" || break; done < /tmp/picks.txt
```

- On a conflict: resolve, `git add`, `git cherry-pick --continue`, then rerun the loop
  starting after that hash. Use `git cherry-pick --skip` only for a commit that is
  already on master in another form. If a conflict is not mechanical, stop and ask.
- Skip the dev version bump commits (`Bump dev version to ...-dev`). Skip nothing else,
  including dependency, toolchain, and CI bumps. Skipping them makes master drift from
  dev, and later bumps and the code that relies on them stop applying.
- Bump conflicts (`Cargo.lock`, action pins, Dockerfile toolchain) mean master missed an
  earlier bump. Resolve them by taking dev's side of each conflicting hunk.
- After the last pick, the tree must match dev except for version strings and the
  master-only `docs/redoc/v*/` files:
  `git diff --stat master dev -- . ':!docs/redoc'`. If anything else differs (for
  example bumps an earlier release skipped), bring it over from dev in one commit,
  `Sync dependencies with dev`, keeping master's version strings.
- Check the result builds: `cargo clippy --workspace --all-targets -- -D warnings`.
- Push: `git push origin master`.
- Check CI on the pushed master commit: `gh run list --branch master --commit <sha>`. Some
  workflows run only on master, such as Rust GPU tests, so a master push can be their
  first run on new code. Fix failures with a PR into dev, then cherry-pick the fix into
  master once it merges. Don't fix master directly. Ignore Dependabot runs.
- Before fixing, check whether the failure is a flake that the last release also has
  (the code is unchanged since it, and the test rarely fails in earlier runs). Report it,
  and if the user agrees to treat it as known, rerun with `gh run rerun <id> --failed`.

## 3. Release notes

Collect the PRs from the cherry-picked commits:
`git log --format=%s vPREV..master | grep -o '#[0-9]*'`. Read each PR title/body and
write one line per user-visible change, in this format:

```markdown
# Change log

## Improvements

* https://github.com/qdrant/qdrant/pull/NNNN - Make sparse vector upserts significantly faster

## Bug Fixes

* https://github.com/qdrant/qdrant/pull/NNNN, https://github.com/qdrant/qdrant/pull/MMMM - Fix group_by on paths with an array index

## Security

* https://github.com/qdrant/qdrant/pull/NNNN - Accept only read-write API keys on the internal gRPC API
```

- Describe the effect for users, not the implementation. Start with a verb.
- Group several PRs of one change on one line. Most impactful first in each section.
- New features stay out of the notes, except features enabled by default that change
  existing behavior (check the defaults in `lib/common/common/src/flags.rs`). List those
  as improvements. Don't put the flag or env variable that disables them in the notes,
  but confirm that `QDRANT__FEATURE_FLAGS__<FLAG_NAME>=false` disables them, in case a
  user needs to.
- A change made of more than 2 PRs links its GitHub milestone instead of the PRs. If
  the PRs have no milestone yet, create one named after the feature and assign them:
  `gh api repos/qdrant/qdrant/milestones -f title="..."`, then
  `gh api -X PATCH repos/qdrant/qdrant/issues/NNNN -F milestone=<number>`.
- Leave out internal-only changes: CI, tests, refactors, dependency bumps (unless
  security-relevant), Edge-only changes, docs.
- Put the notes in the master bump PR body. A human creates the GitHub release from them.

## 4. Bump PR into master

```bash
git checkout -b bump-X.Y.Z master
```

Change the version `X.Y.Z-dev` → `X.Y.Z` in:

- `Cargo.toml` (`version`)
- `lib/api/Cargo.toml` (`version`)
- `lib/common/common/src/defaults.rs` (`QDRANT_VERSION_STRING`)
- `Cargo.lock` (run `cargo check -p qdrant` or similar to refresh it)
- `tools/missed_cherry_picks.sh`: set `IGNORE_UPTO` to the full hash of the last dev
  commit cherry-picked in step 2. Commits whose conflicts were resolved get a new
  patch-id, so an older hash would list them again in the next release. Write it with
  `git rev-parse`, never by hand.
- If more commits land on dev before the PR merges and should ship, repeat step 2 for
  them, then update `IGNORE_UPTO` on the bump branch with a new commit. Leave a commit
  out of the notes when it only fixes code that no release contains yet.

Commit as `Bump version to X.Y.Z`, push, and open the PR:

```bash
gh pr create --base master --head bump-X.Y.Z --title "Bump version to X.Y.Z" --body-file notes.md
```

The PR body is `Bump Qdrant to version X.Y.Z.` followed by the release notes (step 3).

## 5. Bump PR into dev

From `dev`, branch `bump-dev-X.Y.(Z+1)`, change the same three version locations plus
`Cargo.lock` to `X.Y.(Z+1)-dev`. Title: `Bump dev version to X.Y.(Z+1)-dev`. Body:
`Bump development version to X.Y.(Z+1)-dev after <link to the master bump PR>.`

## 6. Tag

Once master CI is green and the master bump PR is approved, squash-merge it:

```bash
gh pr merge NNNN --squash --subject "Bump version to X.Y.Z (#NNNN)" --body ""
git checkout master && git pull --ff-only origin master
git log -1 --oneline   # must be "Bump version to X.Y.Z (#NNNN)"
git tag -s vX.Y.Z -m vX.Y.Z
git push origin refs/tags/vX.Y.Z
```

Don't name a branch `vX.Y.Z`: it makes the tag name ambiguous for git. If one exists,
delete it before pushing the tag.

Pushing the tag builds Docker images (`.github/workflows/docker-image.yml`). Publishing the
GitHub release builds binaries (`.github/workflows/release-artifacts.yml`).

Then squash-merge the dev bump PR the same way.
