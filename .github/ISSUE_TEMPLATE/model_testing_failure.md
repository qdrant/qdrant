---
title: Nightly model testing failure
labels: bug, model testing
---

## Last failure

The nightly model testing job failed.

- Seed: `{{ env.SEED }}`
- Failed: {{ env.FAILED_PASSES }}
- Date: {{ date | date('DD.MM.YYYY HH:mm') }}
- [Failed run](https://github.com/{{ env.REPOSITORY }}/actions/runs/{{ env.RUN_ID }})
- [Commit](https://github.com/{{ env.REPOSITORY }}/tree/{{ env.SHA }}) (the `dev` commit under test)

Reproduce (build with `--features "service_debug edge-verify" --profile perf`; add `--edge-verify` if the edge pass failed):

```
target/perf/model_testing \
  --seed {{ env.SEED }} \
  --op-num {{ env.OP_NUM }} \
  --shard-count 2 \
  --restart-probability 0.001 \
  --id-pool 1000 \
  --on-disk \
  --async-scorer
```

Note: this issue is reused for every nightly failure, so the details above describe the
**latest** one only. Consecutive failures are often unrelated bugs; check the linked run.
