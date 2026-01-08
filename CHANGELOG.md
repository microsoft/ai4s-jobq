CHANGELOG
=========

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

* Collect exceptions from threadpoolexecution
* Azureml job could not be copied and failed silently

2.13.0 (2025-11-05)
-------------------

Features:

* Workforce now has a function `get_available_to_hire`, which determines how many workers can be hired for azureml clusters. This can be used for smarter scheduling.
* Multiregion-Workforce: instead of calculating the available_to_hire itself for AML clusters, it now uses the unified `Workforce.get_available_to_hire`


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
* Expose logging setup for for SDK (not CLI) users via `ai4s.jobq.setup_logging`.

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
* explicitly log task_canceled events to log analytics.
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

* log when SIGTERM is received (eg during preemption)

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

  You may need to e.g. manually initialize conda in each command before you can conda activate an environment.


1.13.1 (2025-02-25)
-------------------

Fixes:

* fix bug that prevented jobs from reappearing in the queue after a worker is preempted.
* tasks were sometimes canceled but not awaited, resulting in potentially unecessary verbose/scary exits


1.13.0 (2025-02-13)
-------------------

Fixes:

* logging of number of succeeded/failed tasks to mlflow was incorrect when used with multiple ayncio workers per process. Now, the correct number of tasks is logged.

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

  - concurrent queueing isn't supported, added lock
  - service-side locking wasn't working, explicitly registered peek-locked messages

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

* Logging in storage_queue complained about unconverted argument

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
  for call_in_config integration.

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
