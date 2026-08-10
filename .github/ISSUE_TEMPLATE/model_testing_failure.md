---
title: Nightly model testing failure
labels: bug, model testing
---

## Last failure

The nightly model testing job failed.

- Seed: `{{ env.SEED }}` (reproduce with `--seed {{ env.SEED }}`)
- Date: {{ date | date('DD.MM.YYYY HH:mm') }}
- Failed: {{ env.FAILED_PASSES }}
- [Failed run](https://github.com/{{ env.REPOSITORY }}/actions/runs/{{ env.RUN_ID }})
- [Commit](https://github.com/{{ env.REPOSITORY }}/tree/{{ env.SHA }}) (the `dev` commit under test)

Note: this issue is reused for every nightly failure, so the details above describe the
**latest** one only. Consecutive failures are often unrelated bugs; check the linked run.
