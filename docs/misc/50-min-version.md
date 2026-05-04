# Minimum package version gate

## Overview

When pushing a job, you can require that workers have a minimum version of a
specific package installed before they execute it. Workers that do not meet the
requirement automatically requeue the job so that a sufficiently up-to-date
worker can pick it up.

This is useful during rolling upgrades: you can start pushing jobs that depend
on new features while older workers are still draining the queue.

## CLI usage

Pass a [PEP 508](https://peps.python.org/pep-0508/) requirement specifier to
`--min-version`:

```bash
ai4s-jobq $QUEUE push -c "my-tool run --flag" --min-version "my-tool>=1.2"
```

Workers with `my-tool` older than 1.2 (or without it installed) will requeue
the job instead of executing it.

Any valid version specifier works, for example `"my-tool>=1.2,<3.0"` or
`"my-tool~=1.2"`.

## Python API

Both `JobQ.push` and `batch_enqueue` accept a `min_version` keyword argument:

```python
# Single push
await queue.push({"cmd": "my-tool run --flag"}, min_version="my-tool>=1.2")

# Batch enqueue
from ai4s.jobq import batch_enqueue

await batch_enqueue(
    queue,
    work_spec,
    num_retries=3,
    min_version="my-tool>=1.2",
)
```

A common pattern is to pin the minimum version to whatever the pusher is
currently running:

```python
from importlib.metadata import version

await queue.push(
    {"cmd": "my-tool run --flag"},
    min_version=f"my-tool>={version('my-tool')}",
)
```

## How it works

1. The `min_version` string is stored as an optional field on the task message.
2. When a worker pulls the task, it parses the requirement specifier and looks
   up the installed version of the named package via `importlib.metadata`.
3. If the installed version does not satisfy the specifier (or the package is
   not installed), the worker requeues the message and backs off for a few
   seconds.
4. If the installed version satisfies the specifier, the task executes normally.

## Worker shutdown on repeated version mismatches

If a worker encounters too many consecutive version-gate requeues without
successfully executing any task, it shuts down to free the compute slot for a
newer worker. The threshold defaults to 100 and can be configured via the
`JOBQ_MAX_VERSION_REQUEUES` environment variable:

```bash
export JOBQ_MAX_VERSION_REQUEUES=50
```

Version-gate requeues do not count toward the regular `max_consecutive_failures`
threshold.

## Backward compatibility

| Scenario | Behavior |
|---|---|
| Old worker receives job with `min_version` | Ignores the field, executes the job. Acceptable during fleet transition. |
| New worker receives job without `min_version` | No check is performed, executes normally. |
| All workers too old | Job bounces between workers. The backoff after requeue prevents excessive load. The job eventually expires via message TTL. |
