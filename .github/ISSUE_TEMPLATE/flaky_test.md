---
title: Flaky test `{{ env.TEST_NAME }}`
---

## Last report

### System error

```text
{{ env.SYSTEM_ERROR }}
```

### Context

Date: {{ date | date('DD.MM.YYYY HH:mm') }}

[Flaky failure run](https://github.com/{{ env.REPOSITORY }}/actions/runs/{{ env.RUN_ID }}/job/{{ env.JOB_ID }})

[Commit](https://github.com/{{ env.REPOSITORY }}/tree/{{ env.SHA }})

OS: {{ env.OS }}

Branch: {{ env.BRANCH }}

{{ env.PR }}
