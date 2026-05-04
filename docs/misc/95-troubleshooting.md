# Troubleshooting

Symptom-first guide for failures encountered when running `Workforce` /
`MultiRegionWorkforce` against many AML regions. Each entry has a
**Symptom** (what you will see), a **Cause**, and a **Fix**.

## File-descriptor exhaustion (`RLIMIT_NOFILE`)

**Symptom.** The process does not crash with a clean error. Instead, you
see a cascade roughly like:

- Background log noise about `BlockingIOError: [Errno 11] Resource
  temporarily unavailable` or `OSError: [Errno 24] Too many open files`.
- `AzureCliCredential` token refreshes fail because `subprocess` cannot
  fork `/usr/bin/az`.
- The cached AAD token expires.
- AML then returns `403 not having read/browse access to ... runs` for
  every region, even though the identity has access.

**Cause.** Each reader thread holds several keep-alive HTTPS sockets plus
the credential subprocess pipe. The Ubuntu default `RLIMIT_NOFILE=1024`
for non-login shells (which is what `tmux` inherits) is reached quickly
once you fan out across 10+ regions with `parallel_region_reads=True`.

**Fix.** Raise the soft limit *in the shell that launches the workforce*,
before starting `tmux` or the autoscaling loop:

```bash
ulimit -n 65536
python multiregion_workforce_dft.py run-forever --parallel-region-reads
```

`65536` is a safe baseline. Verify with `ulimit -n` inside the
running shell, and inside `tmux` once attached.

## Slow tick / per-region calls dominate runtime

**Symptom.** A single autoscaling tick takes many minutes. The per-phase
summary line shows that the read-only phases (`get_current_state`,
`get_available_to_hire`, resume discovery) account for almost all of
the wall-clock time, with the slowest 3 regions clustered near the top
of every phase.

**Cause.** The default `MultiRegionWorkforce` runs all per-region
read-only calls sequentially. Latency adds up linearly with fleet size.

**Fix.** Pass `parallel_region_reads=True` to the
`MultiRegionWorkforce` constructor (or `--parallel-region-reads` on the
CLI). Reads are then fanned out across a thread pool sized as
`n // 5 + 1`, capped at 32. Writer phases stay outer-sequential because
each already runs an inner 8-thread pool. Make sure to apply the
[file-descriptor fix](#file-descriptor-exhaustion-rlimit_nofile) at the
same time.

## ProcessPool child hangs forever

**Symptom.** A worker dispatched through `ProcessPool` never returns.
The parent is healthy but child stdout is silent. `py-spy dump` on the
child shows it stuck inside `logging.Handler.emit` or an Azure SDK
import.

**Cause.** On Linux the default multiprocessing start method is `fork`,
which clones every parent lock—including C-level locks held by
parent threads (Azure SDK credential cache, MSAL token refresh, OpenSSL).
Those locks remain held in the child, where the owning thread does not
exist, so any code that touches them deadlocks.

**Fix.** This is fixed in production code: `ProcessPool` uses
`_safe_mp_context()` which returns `forkserver` on Linux/macOS and
`spawn` on Windows. If you wrap your own `multiprocessing.Pool`,
explicitly request a non-`fork` context:

```python
import multiprocessing as mp
ctx = mp.get_context("forkserver")
pool = ctx.Pool(...)
```

## `403 not having read/browse access to ... runs` for one region

**Symptom.** AML rejects every read against one region with a `403`,
but the identity has the right RBAC role on that region's workspace.

**Cause.** Almost always the credential cascade described in
[file-descriptor exhaustion](#file-descriptor-exhaustion-rlimit_nofile).
Less commonly, the workspace was rotated and the cached MSAL token
points at a stale tenant or client id.

**Fix.** Apply the FD fix first. If `403`s persist, force a fresh
token: `az account clear && az login`, then restart the workforce.
