---
title: Nightly model testing failure
labels: bug
---

## Last failure

The nightly model testing job failed.

- Seed: `{{ env.SEED }}` (reproduce with `--seed {{ env.SEED }}`)
- Date: {{ date | date('DD.MM.YYYY HH:mm') }}
- [Failed run](https://github.com/{{ env.REPOSITORY }}/actions/runs/{{ env.RUN_ID }})
- [Commit](https://github.com/{{ env.REPOSITORY }}/tree/{{ env.SHA }})
