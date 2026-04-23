CHANGELOG
=========


3.9.0 (2026-04-23)
------------------

Features:

* **``Workforce`` parallel hire / lay off / resume.**
  Added ``parallel_hire``, ``parallel_lay_off``, ``resume``, and
  ``parallel_resume`` methods to :class:`Workforce`, mirroring the
  existing ``hire`` / ``lay_off`` signatures. The ``parallel_*`` variants
  use a thread pool (``workers=8`` by default) and surface a Rich progress
  bar when ``progress=True`` (default). Individual submission / cancel /
  resume failures are logged as warnings and do not abort the batch.
  ``resume`` uses the MFE execution REST API (the AzureML ARM API does
  not expose resume). The sequential ``hire`` and ``lay_off`` methods
  also gained a ``progress: bool = True`` keyword argument with the same
  progress bar, so long-running sequential runs no longer appear frozen.

  For thread safety, ``parallel_hire`` builds each worker from a shallow
  copy of the prototype ``Command`` with a fresh ``environment_variables``
  dict, so concurrent submissions no longer race on shared attributes.

  ``_resume_one`` retries transient 5xx responses (including the
  ``500``-wrapped ``503 DatabaseOverCapacity`` returned by Singularity's
  CJP under heavy load) up to three times, honoring ``Retry-After`` /
  ``x-ms-retry-after-ms`` with exponential-backoff fallback.

  ``lay_off`` (and ``parallel_lay_off``) now prefer to cancel ``Paused``
  jobs first (followed by ``Queued``, ``Waiting``, ``Preparing``,
  ``Starting``, then ``Running``). Paused jobs on Singularity have
  typically already hit the max-execution-time cap and cannot be
  resumed, so cancelling them is the only way to free the slot; active
  states are cancelled last to avoid discarding in-flight work.

  ``hire`` and ``parallel_hire`` now tolerate the AzureML
  ``JobPropertyImmutable`` error on our own freshly generated job names.
  This error is only reachable via the azure-core transport retry
  policy re-sending a ``create_or_update`` whose first attempt already
  succeeded server-side, so the job *is* created even though the client
  sees a failure. The retry race is now detected and treated as
  success. Previously the sequential ``hire`` loop had no
  per-iteration error handling, so a single such race would abort the
  entire batch.

* **``MultiRegionWorkforce`` parallel hires and per-tick resume.**
  MRW now loops regions sequentially and delegates to
  ``Workforce.parallel_hire`` / ``parallel_lay_off`` (8 threads
  internally). This better handles uneven hiring distributions: a
  region that needs 5 hires no longer blocks one that needs 500.
  Applies uniformly to ``run()``, ``run(scale_to_zero=True)``,
  ``run(manual_hire=n)`` (new ``_apply_uniform_change`` helper), and
  ``layoff_queued_workers``.

  ``run()`` now resumes paused workers in every region at the end of
  each tick via a new ``_resume_all_regions`` helper. This is cheap
  when nothing is paused (one ``list_jobs`` per region) and prevents
  the running pool from bleeding away on Singularity where jobs hit
  max-execution-time caps between ticks.

* **``Workforce.parallel_hire_in_batches`` and MRW
  ``batched_delay_in_hiring`` flag.**
  New ``Workforce.parallel_hire_in_batches(n, batch_size=512,
  delay=10.0)`` method splits ``n`` hires into fixed-size batches,
  dispatches each batch through ``parallel_hire`` (preserving the
  existing first-hire artifact priming, 8-thread pool, and progress
  bar), and sleeps ``delay`` seconds between batches. Avoids
  overloading the AzureML MFE / container registry on very large
  hires. ``MultiRegionWorkforce`` gained a ``batched_delay_in_hiring:
  bool = True`` constructor argument; when set (default), every MRW hire
  path routes through ``parallel_hire_in_batches`` instead of a
  single ``parallel_hire`` burst. Lay-off and resume paths are
  unchanged.

Fixes / resilience:

* **``Workforce.list_jobs`` retries transient 5xx / 429 responses.**
  The AzureML index endpoint behind ``ml.azure.com`` occasionally
  returns raw nginx 503 HTML during regional load spikes. ``list_jobs``
  now retries up to ``_LIST_JOBS_MAX_RETRIES=3`` times, honoring
  ``Retry-After`` / ``x-ms-retry-after-ms`` with exponential-backoff
  fallback (same helper already used by ``_resume_one``). Previously
  any non-200 response aborted ``get_current_state`` / ``get_detailed_state``
  / scaling with a ``RuntimeError``.

* **``Workforce`` retries network-level request errors.**
  A new ``_request_with_retry`` helper wraps ``list_jobs`` and
  ``get_compute_infos`` so transient ``requests.RequestException``
  errors (DNS ``NameResolutionError``, connection resets, timeouts)
  are now retried with the same Retry-After / back-off logic as HTTP
  5xx / 429. Previously these errors bypassed the HTTP retry loop and
  propagated up to the per-region fallback on the first failure.

* **``Workforce.get_compute_infos`` retries and is timed.**
  ``get_available_to_hire`` calls the ARM management API, which has
  been observed to stall silently for tens of seconds on heavily
  loaded workspaces. ``get_compute_infos`` now goes through the same
  retry helper and MRW logs per-region wall-clock for the capacity
  query, so ticks no longer appear to hang between "Scaling to X"
  and the first hire.

* **``MultiRegionWorkforce.states`` tolerates per-region failures.**
  If ``get_current_state`` raises for a single region (for example, retries
  exhausted), MRW now falls back to that workforce's last-known-good
  state cached in ``_last_good_state``, or to a zero ``State`` if no
  successful reading exists yet. One flaky region no longer aborts
  the whole autoscaling tick.

* **Lock-loss events no longer count as consecutive failures.**
  Added ``LockLostError`` exception for Service Bus lock-loss events
  (expired locks, 404 on renewal). Previously these were counted as
  consecutive failures, eventually shutting down healthy workers. The
  worker now skips the failure counter and continues to the next task.

* **Lock duration no longer drops to 30 s after first renewal.**
  The renewal loop was overwriting its lock duration with the value
  returned by ``renew_lock()``, but the Service Bus REST API returns
  empty ``BrokerProperties`` on renewal responses, causing a fallback
  to the 30 s default. The original lock duration from the initial
  peek-lock is now kept for the entire renewal loop lifetime.

* **Orphaned child processes no longer hang workers.**
  Rewrote the orphan detection to poll ``process.returncode`` instead
  of ``os.waitpid(WNOHANG)``, which raced with asyncio's
  ``ThreadedChildWatcher``. Added a drain timeout with process group
  termination for inherited-pipe scenarios.

* **SIGKILL escalation for unresponsive subprocesses.**
  When a subprocess ignores SIGTERM, the worker now escalates to
  SIGKILL after ``JOBQ_KILL_TIMEOUT`` seconds (default: 600). Each
  subprocess runs in its own process group so the signal reaches all
  descendants without affecting the worker.

Internal / observability:

* **Richer ``Workforce`` progress bars.**
  Progress bars (``hire``, ``parallel_hire``, ``lay_off``,
  ``parallel_lay_off``, ``resume``, ``parallel_resume``) now show
  percent complete, a live items-per-second ``_RateColumn``, live
  ``✓`` / ``✗`` counters inside the description, and the workforce's
  experiment name so bars are self-identifying when a caller loops
  over many regions. Columns are unified across sequential and
  parallel variants via a new ``_make_progress`` helper.

* **Per-region MRW logs.**
  ``run()`` now logs a ``Tick start`` line, marks every region with
  ``[i/N] name:``, emits per-region wall-clock timings for state
  fetch, capacity query, hire, and resume phases, and closes each
  tick with a summary (``Tick done across K region(s) in Ys: hired
  X/Y``). The same structure applies to scale-to-zero, manual
  hire / lay-off, and the paused-worker resume pass.

* **Rich console sharing between progress bar and log handler.**
  The progress bar and ``RichHandler`` now share a single ``Console``
  instance, preventing interleaved output.

* **Timestamp diagnostics for live Service Bus tests.**
  Added timestamps to all live tests for easier debugging of
  timing-sensitive scenarios.

* **Fixed graceful shutdown test failures.**
  Fixed subprocess ``env`` dict not inheriting the parent environment,
  and fixed assertions broken by Rich line-wrapping of long log
  messages.


3.8.0 (2026-04-21)
------------------

Fixes:

* **Service Bus ``replace()`` no longer causes message buildup.**
  The previous implementation sent a new message without deleting the old one,
  causing the queue to grow with every task failure. ``replace()`` is now a
  no-op on Service Bus—the message is re-delivered with its original content
  after the lock expires. Storage Queue behavior is unchanged (atomic
  in-place update).


3.7.1 (2026-04-17)
------------------

Fixes:

* **Fix Service Bus REST ``peek_messages()`` using AMQP SDK.**
  The REST API ``peekonly=true`` parameter behaves inconsistently across
  Service Bus namespace tiers—on some it returns 404, on others it
  silently locks the message instead of peeking. Replaced with the native
  AMQP-based ``ServiceBusReceiver.peek_messages`` for true non-destructive
  peek (no locks, no delivery-count increment).

* **Retry transient 404 in ``peek_lock_message()``.**
  After queue operations the Service Bus data-plane may briefly return 404.
  ``peek_lock_message`` now retries up to 3 times with a 2 s back-off.

Internal:

* **Enable strict linting, type checking, and CI hardening.**
  Added 25+ ruff rule groups, strict mypy configuration, split CI into
  lint / test / docs jobs, added PR review workflow and CODEOWNERS.

* **Add doc8, markdownlint, and Vale documentation linters.**
  Added three documentation linters as pre-commit hooks and a CI job:
  doc8 for RST structure, markdownlint for Markdown formatting, and Vale
  for prose style (Google style base with custom JobQ rules and vocabulary).
  Fixed all existing lint issues including Latin abbreviation replacements,
  em-dash spacing, typos, and heading hierarchy.

3.7.0 (2026-04-16)
------------------

Breaking changes:

* **Rename ``Workforce.State.num_queued`` → ``num_pending``.**
  The field now accurately reflects that it counts all non-running active jobs
  (Queued, Preparing, Paused, Starting, Waiting), not just queued ones.
  Consumers accessing ``.num_queued`` on ``State`` must update to ``.num_pending``.

Features:

* **Add ``DetailedState`` with per-status breakdowns.**
  New ``DetailedState(State)`` subclass exposes ``num_paused``, ``num_queued``,
  ``num_preparing``, ``num_starting``, and ``num_waiting`` fields.
  Retrieve via ``Workforce.get_detailed_state()`` at no extra API cost.

3.6.1 (2026-04-16)
------------------

Fixes:

* **Suppress OTel exporter log spam after preemption.**
  Set the ``azure.monitor.opentelemetry.exporter.export._base`` logger to
  ``CRITICAL`` so that failed telemetry exports no longer flood user logs
  with tracebacks when the AppInsights endpoint is unreachable.

3.6.0 (2026-04-09)
------------------

Feature:

* **Reduce noise when scheduling.**
  ``batch_enqueue()`` now logs queue-completion and worker-join messages at
  ``DEBUG`` level instead of ``INFO`` when ``show_progress`` is disabled.

* **Improve Azure Monitor configuration.**
  Suppress ``AppDependencies`` table noise with ``sampling_ratio=0.0``,
  disable performance counters and live metrics by default, find the Azure
  Add exception filtering to ``CustomDimensionsFilter`` to redirect
  exceptions from ``AppExceptions`` to ``AppTraces``.

3.5.0 (2026-04-07)
------------------

Fixes:

* **ServiceBus REST: non-blocking pool shutdown during preemption.**
  ``ProcessPool._kill_subprocesses`` now runs ``pool.shutdown(wait=True)``
  in a thread executor instead of blocking the asyncio event loop. This
  keeps lock-renewal tasks and heartbeats alive while waiting for pool
  processes to exit, preventing message-lock expiration on the ServiceBus
  REST backend (5-minute lock duration).

* **ServiceBus REST: ``deadletter_message()`` timeout and error handling.**
  The one-off AMQP connection opened for dead-lettering now has a 30-second
  timeout and catches all errors instead of letting a hung or failed AMQP
  connection break the worker.

* **Resilient ``requeue()`` on worker cancellation.**
  A failed ``requeue()`` call (for example, expired lock) inside the
  ``WorkerCanceled`` handler no longer replaces the exception and kills
  the worker—the error is logged and the cancellation flow continues.

* **Non-blocking ``ProcessPool.__aexit__``.**
  Final pool shutdown in ``__aexit__`` also runs via ``run_in_executor``
  for consistency.

3.4.0 (2026-03-30)
------------------

Features:

* Listen for preemptions on native Azure Batch.

3.3.0 (2026-03-24)
------------------

Features:

* Set `job_url` from environment variable if not running on azureml, so that the grafana dashboard links to the correct job details page even for non-azureml jobs.
* Moved ``worker_id`` and other per-call logging extras into
  ``CustomDimensionsFilter`` so they are automatically attached to every log
  record. A new ``set_custom_dimensions()`` helper in ``logging_utils``
  lets callers register process-wide dimensions once instead of threading
  them through every ``extra={…}`` dict.

* Added ``set_context_dimensions()`` for per-coroutine logging dimensions
  using ``contextvars``.

* ``CustomDimensionsFilter`` is now always attached to the ``LOG`` and
  ``TASK_LOG`` loggers (previously it was only created when an Application
  Insights connection string was present).

Fixes:

* Each async worker now gets a unique ``worker_id``
  (``<node_id>:<idx>`` when ``num_workers > 1``) so log records can be
  attributed to individual workers rather than just the node.

3.2.0 (2026-03-20)
------------------

Features:

* Include ``azureml_workspace_name`` in Application Insights custom dimensions
  even when ``AZUREML_RUN_ID`` is not set (for example, AzureML batch jobs), so
  Grafana dashboards can filter by workspace for all job types.

Fixes:

* Handle ``tenacity.RetryError`` in the worker heartbeat so that transient
  failures in ``get_approximate_size()`` no longer kill the heartbeat task
  (previously surfaced as "Task exception was never retrieved").

* Increase retry attempts for ``get_approximate_size()`` from 3 to 10 to
  better tolerate transient Service Bus admin API failures.

3.1.0 (2026-03-19)
------------------

Features:

* Added GitHub Copilot skill support. A CLI reference and documentation
  bundle is now auto-installed to ``~/.copilot/skills/ai4s-jobq-cli/`` on
  first invocation, giving Copilot rich context about every ``ai4s-jobq``
  subcommand.

* New ``copilot-skill`` CLI subcommand group (``install``, ``clear``,
  ``list``) for manual skill management.

* New ``scripts/build_skill.py`` build script that generates ``SKILL.md``
  from the asyncclick command tree and bundles Sphinx-built documentation
  references into the package.

* ``BACKEND_SPEC`` is now optional for the top-level CLI group, allowing
  subcommands like ``copilot-skill`` to run without a queue connection.

Fixes:

* Fixed inconsistent ``queue`` property in Application Insights logs.
  ``LOG.exception`` and ``LOG.info`` for task failures/retries explicitly set
  ``queue`` to ``self.full_name`` (for example, ``livdft.servicebus.windows.net/…``),
  overriding the ``sb://…`` format set by the ``CustomDimensionsFilter``.
  Removed the redundant overrides so all log events use the same short format.


3.0.2 (2026-03-19)
------------------

Fixes:

* Fixed dead-lettering in the Service Bus REST backend. The REST API does
  not support explicit dead-lettering—the previous implementation silently
  abandoned messages instead, causing them to cycle back into the main queue
  until ``MaxDeliveryCount`` was hit. Dead-lettering now uses a one-off AMQP
  management link to settle the message using the lock token already held by
  the REST peek-lock, which is atomic and race-free.

* New queues are now created with ``max_delivery_count=1000`` to prevent
  Service Bus auto-dead-lettering from interfering with application-level
  retry logic.


3.0.1 (2026-03-17)
------------------

Fixes:

* Fixed retry logic in ``ServiceBusRestBackend.__len__``: the ``AttributeError``
  raised when Azure returns ``None`` for ``message_count_details`` is now caught
  and converted to a retryable ``None`` result, so ``tenacity`` actually retries
  instead of immediately propagating the exception.

* Fixed ``_CachedTokenCredential`` closing the caller-owned credential transport.
  ``__aenter__``/``__aexit__`` no longer delegate to the wrapped credential,
  preventing "HTTP transport has already been closed" errors when the same
  credential is reused across multiple ``ServiceBusRestBackend`` context entries.


3.0.0 (2026-02-24)
------------------

Breaking changes:

* Service Bus queues are now created with duplicate detection enabled
  (``requires_duplicate_detection=True``) and a 7-day detection window.
  Existing queues must be deleted and recreated to benefit from deduplication.

Features:

* Task IDs are now deterministic by default (``JOBQ_DETERMINISTIC_IDS=true``).
  Identical tasks produce the same ID (MD5 of serialized content), enabling
  Service Bus duplicate detection to silently drop re-submitted tasks.

* A warning is now logged on startup when the queue's duplicate detection
  setting does not match the ``JOBQ_DETERMINISTIC_IDS`` configuration—in
  either direction.

* Application Insights with RBAC authentication is now supported. The credential
  validation now uses the correct ``https://monitor.azure.com/.default`` scope.

* Added retry logic for ``get_queue_runtime_properties`` in the Service Bus REST
  backend to handle transient ``None`` responses that could cause ``AttributeError``.

* Changed log format to ``YYYY-MM-DD HH:MM:SS LEVEL: message [logger]`` for better
  readability and consistency.


Fixes:

* Fixed MLflow import error when using Azure ML tracking URIs by setting
  ``MLFLOW_REGISTRY_URI`` to empty before import, preventing the
  ``UnsupportedModelRegistryStoreURIException``.


2.17.0 (2026-02-12)
-------------------

Features:

* **Service Bus backend rewritten from AMQP to REST.** The previous AMQP-based
  backend (`servicebus.py`) has been replaced with a new HTTP/REST implementation
  (`servicebus_rest.py`) for improved stability. The new backend uses `aiohttp`
  with `tenacity` for automatic retries of transient errors.

* When a message lock is lost (Service Bus 404 or Storage Queue pop receipt mismatch),
  the running task is now automatically cancelled and the worker moves on without
  settling the message, avoiding duplicate processing.

* Lock-loss detection added to the Storage Queue backend: heartbeat renewal failures
  with HTTP 400/404 now signal lock loss via the new `lock_lost_event` on `Envelope`.

Breaking changes:

* The `[servicebus]` pip extras group has been removed; `azure-servicebus` is now
  a core dependency.
* `tenacity` is now a core dependency.

Fixes:

* fix ANSI escape codes breaking subprocess output assertions in CLI tests

2.16.2 (2026-01-27)
-------------------

* catch ProcessLookupError when process we want to kill already ended

2.16.1 (2026-01-22)
------------------

* fix an error for workers running on MacOS


2.16.0 (2026-01-09)
------------------

* make multiregion workforce compatible with service bus backend


2.15.1 (2026-01-08)
-------------------

* fix(servicebus): queue creation was attempted after first access, causing a crash

2.15.0 (2025-12-23)
-------------------

* feat(backends): new servicebus backend

2.14.0 (2025-12-04)
-------------------

* feat(logging): authenticate appinsights if token credential available (#15)
* fix(track): ensure CLI queue is selected at startup (#16)
* doc(api): improved ordering for new users (#17)

2.13.1 (2025-11-06)
-------------------

Fix:

* Collect exceptions from `ThreadPoolExecution`
* Azureml job could not be copied and failed silently

2.13.0 (2025-11-05)
-------------------

Features:

* Workforce now has a function `get_available_to_hire`, which determines how many workers can be hired for azureml clusters. This can be used for smarter scheduling.
* Multiregion-Workforce: instead of calculating `available_to_hire` itself for AML clusters, it now uses the unified `Workforce.get_available_to_hire`


2.12.0 (2025-11-04)
-------------------

Features:

* force flush messages to app insights when canceled


2.11.1 (2025-10-21)
-------------------

Fixes:

* in track, tz-aware and tz-unaware datetimes were subtracted

2.11.0 (2025-10-10)
-------------------

Features:

* When `--time-limit` is reached, initiate a clean shutdown sequence where tasks are signaled SIGTERM
  and can checkpoint.

2.10.0 (2025-09-26)
-------------------

Features:

* Storage queue backend now supports a dead letter queue
* Apply custom dimensions filter to azure log analytics logging, which adds job/worker meta data to each log line.
* Expose logging setup for SDK (not CLI) users via `ai4s.jobq.setup_logging`.

2.9.0 (2025-09-05)
-------------------

Features:

* the workforce monitor now supports two shutdown modes: `do-not-accept-new-tasks` and `graceful-downscale`.
  In `do-not-accept-new-tasks` mode, the monitor will wait until all workers are idle before shutting down.
  This is useful when you want to scale down without interrupting running tasks. In case of `graceful-downscale`, the monitor will send `SIGTERM` to all running workers, and wait some time so that they can write a checkpoint before being killed.
  To use this mode, set the environment variable `JOBQ_WORKFORCE_SHUTDOWN_MODE=graceful-downscale`.

Fixes:

* workforce monitor task did not cancel when the queue was empty


2.8.0 (2025-08-26)
-------------------

Features:

* add feature to workforce to automatically create service bus topic and subscription if parameters are provided
* add feature to multiregion workforce to scale down by laying off queued workers


2.7.0 (2025-07-18)
-------------------

Features:

* Implemented workforce monitor to listen and handle the incoming workforce control events from the service bus.
* Implemented support for graceful downscale events.

2.6.1 (2025-07-18)
------------------

Features:

* fix parameter type mismatch in `timeout` parameter of `QueueClient.update_message` function

2.6.0 (2025-07-16)
------------------

Features:

* add jobq track UI

2.5.4 (2025-07-15)
------------------

Features:

* Add extras field to PreemptionEventHandler to allow visualisation in the grafana dashboard.


2.5.3 (2025-07-10)
------------------

Features:

* log metadata to log analytics for every record.
* explicitly log `task_canceled` events to log analytics.
* avoid line breaks in non-interactive environment


2.5.2 (2025-06-26)
------------------

Fixes:

* When an announced aml compute preemption does not occur, continue processing tasks.

2.5.1 (2025-06-13)
-------------------

Fixes:

* Jobs now sleep after preemption so that AML ends up ending the job and rescheduling it, instead of thinking it finished.
* Clean up dangling tasks waiting for shutdown events

Misc:

* Add `ai4s.jobq.__version__` to the package, and print version info when starting workers.


2.5.0 (2025-05-28)
------------------

Features:

* Automatically set PYTHONUNBUFFERED=1 in the worker environment to ensure that all output is flushed immediately.
  Note that this can be overwritten by the user by setting an empty value for this envvar when queueing a task.

* New option `--emulate-tty/-t` for worker, that should fix buffering issues
  even with third party / non-python programs in the user task.
  Note that `sys.stdin.isatty()` will return `True` when this option is used,
  so configurations for progress bars that rely on this to detect whether the task
  is running interactively will not work as expected.


2.4.1 (2025-06-05)
-------------------

Fixes:

* Fixed unsupported operand type error in service bus backend


2.4.0 (2025-06-03)
------------------

Features:

* Poll for and handle preemption events on AML clusters (by polling AML endpoint).
  For tasks, this unifies the preemption handling of Singularity and AML,
  they just need to implement a SIGTERM handler.

Fixes:

* Only handle SIGTERM once in the worker process. This was broken when multiple
  `ShellCommandProcessor`s were launched in parallel.

* Add hard exit on second SIGINT (ctrl-c).

* Ensure that all task outputs are logged (to aml/stdout) before closing the
  logging queue and exiting the worker process.


2.3.4 (2025-06-03)
-------------------

Fixes:

* Fixed the incompatible type error in service bus backend

2.3.3 (2025-05-30)
-------------------

Fixes:

* Fixed the name of AMLT_DIRSYNC_EXCLUDE environment variable


2.3.2 (2025-05-022)
------------------

* Add more information about the running AzureML job to worker logs.


2.3.1 (2025-05-22)
------------------

Misc:

* Add timestamps to log messages when not running in an interactive terminal

2.3.0 (2025-05-06)
------------------

Features:

* log when SIGTERM is received (for example, during preemption)

2.3.0 (2025-05-07)
------------------

Features:

* On preemption, make current task pop up in queue again, immediately.

2.2.0 (2025-04-07)
------------------

Misc:

* when job fails, send last 100 log lines to log workspace for dashboard

2.1.0 (2025-04-04)
------------------

Features:

* Allow specifying custom processor class on CLI

2.0.0 (2025-03-26)
------------------

Potentially Breaking Change:

* ShellCommandProcessor: Stop using login shells, since Singularity runs a lot of unwanted commands for login shells.
  If you have to rely on a login shell, set JOBQ_USE_LOGIN_SHELL=true in your worker environment, though it's not recommended.

  You may need to manually initialize conda in each command before you can conda activate an environment.


1.13.1 (2025-02-25)
-------------------

Fixes:

* fix bug that prevented jobs from reappearing in the queue after a worker is preempted.
* tasks were sometimes canceled but not awaited, resulting in potentially unnecessary verbose/scary exits


1.13.0 (2025-02-13)
-------------------

Fixes:

* logging of number of succeeded/failed tasks to mlflow was incorrect when used with multiple asyncio workers per process. Now, the correct number of tasks is logged.

1.12.0 (2025-01-29)
-------------------

Fixes:

* change the logging settings to not log every http request

1.11.1 (2025-02-07)
-------------------

Fixes:

* prevent grafana heartbeat crash on DNS issues by handling corresponding exception

1.11.0 (2025-01-23)
-------------------

Fixes:

* service bus backend was broken in a few ways:

  * concurrent queueing isn't supported, added lock
  * service-side locking wasn't working, explicitly registered peek-locked messages

* ai4s-jobq amlt: remove tmpfile after submit

Features:

* service bus backend allows to peek *all* messages, optionally in json format


1.10.0 (2024-08-19)
-------------------

Misc:

* remove jobq credential. Not bumping major version, since everyone is already successfully using the package without the credential due to changes in security policies.

1.9.0 (2024-05-27)
------------------

Features:

* pass `_worker_id` to user callback when the callback has a parameter with this name

1.8.1 (2024-05-27)
------------------

Fixes:

* Logging in `storage_queue` complained about unconverted argument

1.8.0 (2024-05-18)
------------------

* Make heartbeat the default from CLI. Disable via `--no-heartbeat`.

1.7.0 (2024-05-16)
------------------

Features:

* `launch_workers` now provides the same logging as the CLI, to simplify the creation of 'number of active workers' dashboard plots.


1.6.0 (2024-05-17)
------------------

Fixes:

* allow workers to exit cleanly when an exception occurs during batch enqueue
* add signal handling (this is not yet functional, waiting for AML to do their part)



1.5.0 (2024-05-06)
------------------

Features:

* New `download_folder` function in `blob.py`

Fixes:

* prepend a `cd` command to `cmd` in the ShellCommandLauncher to ensure correct working directory even if AML changed `/etc/profile`.

1.4.1 (2024-04-25)
------------------

Fixes:

* amlt subcommand did not join the subprocess

1.4.0 (2024-04-19)
------------------

Features:

* Simplify entry point when only sequential computing is needed
* Inject jobq env vars into amlt config when using amlt subcommand
* Allow authentication with user-assigned identity on AML clusters rather than keys

1.3.0 (2024-04-16)
------------------

Features:

* When bash is available, use it (as a login shell) to execute the command.
  This allows `conda activate` etc to work provided it has been set up in the
  bashrc.

1.2.4 (2024-04-02)
------------------

Fixes:

* Changed default authentication mechanism from `DefaultAzureCredential` to `AzureCliCredential`.

1.2.3 (2024-04-02)
------------------

Fixes:

* storage queue backend: race conditions when heartbeat got canceled
1.2.3 (2024-04-03)

------------------

1.2.2 (2024-02-27)
------------------

Fixes:

* storage queue backend: deleting tasks failed with error that "reply" is not implemented

1.2.1 (2024-02-26)
------------------

Fixes:

* `ai4s-jobq amlt` crashed when exposing`JOBQ_STORAGE` environment variable


1.2.0 (2024-02-23)
------------------

Features:

* Service Bus backend added. This allows waiting for job results and prepares
  for `call_in_config` integration.

1.1.0
-----

Fixes:

* stricter type checking, fix some type hints

Features:

* new `upload_from_folder` method in `BlobContainer` that allows parallel uploads of files in the folder
* simplified imports from top level package


1.0.1
-----

Fixes:

* any CLI `push` call or python `batch_enqueue()` call cleared the queue. Now, clearing is manual.
* fixed CI test pipeline and tests.


1.0.0
-----

Initial Release
