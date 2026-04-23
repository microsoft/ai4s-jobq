# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import importlib
import json
import logging
import math
import os
import subprocess
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from tempfile import NamedTemporaryFile
from typing import (
    Any,
    AsyncGenerator,
    Iterable,
    TypeVar,
)

import asyncclick as click
import yaml
from azure.core.credentials_async import AsyncTokenCredential

from ai4s.jobq import JobQ, __version__
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.entities import EmptyQueue, LockLostError, WorkerCanceled
from ai4s.jobq.logging_utils import JobQRichHandler, setup_logging
from ai4s.jobq.orchestration import WorkSpecification, batch_enqueue, get_results
from ai4s.jobq.orchestration.manager import launch_workers
from ai4s.jobq.skill_file import skill_file_cmd
from ai4s.jobq.work import DefaultSeed, Processor, ShellCommandProcessor

LOG = logging.getLogger("ai4s.jobq")
TRACK_LOG = logging.getLogger("ai4s.jobq.track")

TaskType = TypeVar("TaskType")
SeedType = TypeVar("SeedType")


@dataclass
class BackendSpec:
    name: str


@dataclass
class ServiceBusSpec(BackendSpec):
    namespace: str

    def __str__(self) -> str:
        return f"sb://{self.namespace}"


@dataclass
class StorageQueueSpec(BackendSpec):
    storage_account: str

    def __str__(self) -> str:
        return self.storage_account


# Commands that don't require a BACKEND_SPEC argument
_NO_BACKEND_COMMANDS = {"copilot-skill"}


class JobQGroup(click.Group):
    """Custom group that allows some subcommands to skip BACKEND_SPEC."""

    def parse_args(self, ctx, args):
        # If the first non-option arg is a command that doesn't need BACKEND_SPEC,
        # insert a placeholder so click doesn't consume the subcommand name as the argument.
        filtered = [a for a in args if not a.startswith("-")]
        if filtered and filtered[0] in _NO_BACKEND_COMMANDS:
            args = list(args)
            # Insert None-placeholder before the subcommand
            idx = args.index(filtered[0])
            args.insert(idx, "__none__")
        return super().parse_args(ctx, args)


class BackendSpecParam(click.ParamType):
    def convert(
        self,
        value: str | BackendSpec | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> BackendSpec | None:
        if value is None or value == "__none__":
            return None
        if isinstance(value, BackendSpec):
            return value
        try:
            location, name = value.rsplit("/", 1)
        except ValueError:
            self.fail(
                f"Expected format: <storage-account>/<queue-name>, got {value!r}",
                param,
                ctx,
            )
        if location.startswith("sb://"):
            return ServiceBusSpec(namespace=location[len("sb://") :], name=name)
        return StorageQueueSpec(storage_account=location, name=name)


class EnvVar(click.ParamType):
    name = "env-var"

    def convert(
        self,
        value: str | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> tuple[str, str] | None:
        if value is None:
            return None
        try:
            key, val = value.split("=", 1)
        except ValueError:
            self.fail(
                f"Expected format: <key>=<value>, got {value!r}",
                param,
                ctx,
            )
        return key, val


class DurationParam(click.ParamType):
    def get_metavar(self, param: click.Parameter) -> str:
        return "<number>[smhdw]"

    def convert(
        self,
        value: timedelta | str | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> timedelta | None:
        if value is None:
            return None

        if isinstance(value, timedelta):
            return value

        last = value[-1]
        if last in "smhdw":
            value = value[:-1]
            if last == "s":
                return timedelta(seconds=int(value))
            if last == "m":
                return timedelta(minutes=int(value))
            if last == "h":
                return timedelta(hours=int(value))
            if last == "d":
                return timedelta(days=int(value))
            if last == "w":
                return timedelta(weeks=int(value))
        return self.fail(f"Expected format: <number>[smhdw], got {value!r}", param, ctx)


class ProcessorParam(click.ParamType):
    def get_metavar(self, param: click.Parameter) -> str:
        return "[shell | map-in-config | <module>.<class>]"

    @staticmethod
    def get_class_from_string(qualified_name: str) -> type[Processor]:
        parts = qualified_name.split(".")
        module_name = ".".join(parts[:-1])
        class_name = parts[-1]
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls  # type: ignore[no-any-return]  # dynamic class loading

    def convert(
        self,
        value: str | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> type[Processor] | None:
        if value is None:
            return None
        if value == "shell":
            return ShellCommandProcessor
        if value == "map-in-config":
            from ai4s.jobq.ext.map_in_config import MapInConfigProcessor

            return MapInConfigProcessor
        return self.get_class_from_string(value)


@dataclass
class QueueConfig:
    backend_spec: BackendSpec | None = None
    conn_str: str | None = None
    credential: str | AsyncTokenCredential | None = None
    log_handler: JobQRichHandler | None = None
    duplicate_detection_window: timedelta | None = None

    @asynccontextmanager
    async def get(
        self, exist_ok: bool = True, require_account_key: bool = False
    ) -> AsyncGenerator["JobQ", None]:
        # instantiate processor
        async with AsyncExitStack() as stack:
            # this is ensured since they come from a click.argument
            assert self.backend_spec is not None

            if self.conn_str is not None:
                queue = await stack.enter_async_context(
                    JobQ.from_connection_string(
                        self.backend_spec.name,
                        connection_string=self.conn_str,
                        exist_ok=exist_ok,
                        credential=self.credential,
                    )
                )
            else:
                if isinstance(self.backend_spec, StorageQueueSpec):
                    credential: str | AsyncTokenCredential | None = self.credential
                    if not credential and not require_account_key:
                        try:
                            credential = await stack.enter_async_context(get_token_credential())
                        except Exception as e:
                            LOG.warning("Authenticating with token credential failed: %s", e)

                    if not credential:
                        raise click.UsageError(
                            "Could not authenticate. Make sure you're logged in via az CLI or run as a managed identity."
                        )
                    queue = await stack.enter_async_context(
                        JobQ.from_storage_queue(
                            self.backend_spec.name,
                            storage_account=self.backend_spec.storage_account,
                            credential=credential,
                            exist_ok=exist_ok,
                        )
                    )
                elif isinstance(self.backend_spec, ServiceBusSpec):
                    credential = self.credential
                    if not self.credential:
                        credential = await stack.enter_async_context(get_token_credential())
                    queue = await stack.enter_async_context(
                        JobQ.from_service_bus(
                            self.backend_spec.name,
                            fqns=f"{self.backend_spec.namespace}.servicebus.windows.net",
                            credential=credential,
                            exist_ok=exist_ok,
                            duplicate_detection_window=self.duplicate_detection_window,
                        )
                    )
                else:
                    raise ValueError(f"Unknown backend spec: {self.backend_spec}")
            yield queue


@click.group(cls=JobQGroup)
@click.argument(
    "backend_spec",
    envvar="JOBQ_STORAGE_QUEUE",
    type=BackendSpecParam(),
    metavar="SERVICE/QUEUE_NAME",
    required=False,
    default=None,
)
@click.option(
    "--conn-str",
    envvar="JOBQ_CONNECTION_STRING",
    hidden=True,
    help="Connection string for the queue",
)
@click.option("--verbose", "-v", count=True, help="Enable verbose logging.")
@click.option("--quiet", "-q", count=True, help="Enable verbose logging.")
@click.pass_context
async def main(
    ctx: click.Context,
    backend_spec: BackendSpec | None,
    verbose: int,
    quiet: int,
    conn_str: str,
) -> None:
    """
    Interact with the job queue, assuming commands are shell commands.

    \b
    SERVICE is one of:
    - sb://<namespace> (Azure Service Bus)
    - <storage-account> (Azure Storage Queue)
    """
    _install_copilot_skill()

    ctx.ensure_object(QueueConfig)

    if backend_spec is not None:
        # log level for ai4s-jobq modules
        internal_log_level = max(logging.DEBUG, logging.INFO - 10 * (verbose - quiet))
        # log level for other modules.
        base_log_level = logging.WARNING
        if abs(verbose - quiet) >= 2:
            diff = int(math.copysign(abs(verbose - quiet) - 2, verbose - quiet))
            base_log_level = max(logging.DEBUG, base_log_level - 10 * diff)

        log_handler = setup_logging(
            f"{backend_spec}/{backend_spec.name}",
            internal_log_level=internal_log_level,
            base_log_level=base_log_level,
        )

        ctx.obj.backend_spec = backend_spec
        ctx.obj.conn_str = conn_str
        ctx.obj.log_handler = log_handler


@main.command("push")
@click.option("--num-retries", default=0, type=int, help="Number of retries.")
@click.option(
    "--num-workers",
    "num_enqueue_workers",
    default=10,
    type=int,
    help="Number of enqueue-workers.",
    show_default=True,
)
@click.option("--command", "-c", multiple=True, help="The command(s) to execute.")
@click.option("--wait", is_flag=True, help="Wait for the job to finish and print its return value.")
@click.option(
    "--bg-dirsync-to", help="Synchronize the $AMLT_DIRSYNC_DIR to this location in the background."
)
@click.option(
    "--env",
    "-e",
    "env_vars",
    type=EnvVar(),
    multiple=True,
    help="Environment variables to set.",
)
@click.option(
    "--dedup-window",
    type=DurationParam(),
    default=None,
    help="Duplicate detection window, e.g. 7d, 12h (Service Bus only, default: 7d).",
)
@click.pass_context
async def push(
    ctx: click.Context,
    command: Iterable[str],
    num_retries: int,
    bg_dirsync_to: str | None,
    env_vars: list[tuple[str, str]],
    wait: bool,
    num_enqueue_workers: int,
    dedup_window: timedelta | None,
) -> None:
    """
    Enqueue a new job to the job queue.
    """
    show_progress = False
    if not command:
        LOG.info("Reading commands from stdin, separated by newline.")
        command = click.open_file("-")
        show_progress = True

    env = dict(env_vars)

    if dedup_window is not None:
        ctx.obj.duplicate_detection_window = dedup_window

    class IteratorWorkSpec(WorkSpecification[dict[str, Any], DefaultSeed]):
        async def list_tasks(
            self, seed: DefaultSeed, force: bool = False
        ) -> AsyncGenerator[dict[str, Any], None]:
            for cmd in command:
                if cmd.startswith("{"):
                    job_spec = json.loads(cmd)
                    job_spec.setdefault("env", {}).update(env)
                    job_spec.setdefault("bg_dirsync_to", bg_dirsync_to)
                    yield job_spec
                else:
                    yield {"cmd": cmd, "bg_dirsync_to": bg_dirsync_to, "env": env}

    async with ctx.obj.get(exist_ok=True) as queue:
        futures = await batch_enqueue(
            queue=queue,
            work_spec=IteratorWorkSpec(),
            num_enqueue_workers=num_enqueue_workers,
            num_retries=num_retries,
            show_progress=show_progress,
            reply_requested=wait,
        )
        if wait:
            LOG.info("Waiting for results...")
            async for result in get_results(futures):
                print(result)


@main.command("peek")
@click.option(
    "-n",
    type=int,
    default=1,
    help="how many messages to show max. For storage queue, <=32 is supported. For service bus, use -1 for 'all messages'",
)
@click.option("--json", "as_json", is_flag=True)
@click.pass_context
async def peek(ctx: click.Context, n: int, as_json: bool) -> None:
    """
    Peek at the next job in the queue.
    """
    try:
        async with ctx.obj.get(exist_ok=True) as queue:
            res = await queue.peek(n, as_json)
            if as_json:
                print(json.dumps(res, indent=2))
            else:
                print(res)
    except EmptyQueue as e:
        LOG.error(str(e))
        raise SystemExit(1) from e


@main.command("sas")
@click.option(
    "--expiry",
    "-e",
    type=DurationParam(),
    default="24h",
    help="Time until SAS token expires.",
)
@click.pass_context
async def sas(ctx: click.Context, expiry: timedelta) -> None:
    """Get a SAS token for the queue."""
    async with ctx.obj.get(exist_ok=True, require_account_key=True) as queue:
        print(await queue.sas_token(ttl=expiry))


@main.command("amlt")
@click.option(
    "--time-limit",
    "-t",
    type=DurationParam(),
    default="24h",
    help="Soft time limit. Tasks will receive SIGTERM when this is reached.",
)
@click.argument("amlt-args", nargs=-1, type=click.UNPROCESSED, metavar="AMLT_ARGS")
@click.pass_context
async def amlt_worker(ctx: click.Context, time_limit: timedelta, amlt_args: list[str]) -> None:
    """Launch Amulet with JOBQ_STORAGE, JOBQ_QUEUE, and JOBQ_TIME_LIMIT set, so they can be referenced in the yaml file.

    Usage:

    \b
      $ ai4s-jobq storage0account/queue0name amlt run my-config.yaml

    So in the easiest case, you could do

    \b
      $ cat commands.txt | ai4s-jobq storage0account/queue0name push
      $ ai4s-jobq storage0account/queue0name amlt -- run config.yaml

    To ensure that jobq can shut down your task cleanly in the case of preemption,
    in the config.yaml, ensure that you launch ai4s-jobq like so:

    \b
      command:
        - ai4s-jobq ... & trap "kill -15 $!" TERM ; wait $!
    """
    amlt_args = list(amlt_args)

    tmpfile = None
    async with AsyncExitStack() as stack:
        config: QueueConfig = ctx.obj
        assert config.backend_spec is not None

        popen_env = os.environ.copy()
        for i in range(len(amlt_args)):
            if amlt_args[i].endswith(".yaml") or amlt_args[i].endswith(".yml"):
                tmpfile = stack.enter_context(
                    NamedTemporaryFile(  # noqa: SIM115 — managed by ExitStack
                        suffix=".yaml",
                        delete=False,
                        dir=os.path.dirname(amlt_args[i]),
                        mode="w",
                    )
                )
                with open(amlt_args[i]) as f:  # noqa: ASYNC230 — sync IO in async CLI handler
                    config_dct = yaml.safe_load(f)
                    if "environment" not in config_dct:
                        # not an amlt yaml
                        continue
                    env = config_dct["environment"].setdefault("env", {})
                    env["JOBQ_STORAGE"] = popen_env["JOBQ_STORAGE"] = str(config.backend_spec)
                    env["JOBQ_QUEUE"] = popen_env["JOBQ_QUEUE"] = config.backend_spec.name
                    env["JOBQ_TIME_LIMIT"] = popen_env["JOBQ_TIME_LIMIT"] = (
                        str(int(time_limit.total_seconds())) + "s"
                    )
                tmpfile.write(yaml.dump(config_dct))
                tmpfile.flush()
                amlt_args[i] = tmpfile.name

        proc = subprocess.Popen(
            ["amlt", *amlt_args],
            stdout=sys.stdout,
            stderr=sys.stderr,
            stdin=sys.stdin,
            env=popen_env,
        )
        proc.wait()
    if tmpfile is not None:
        os.unlink(tmpfile.name)


@main.command("pull")
@click.option(
    "--visibility-timeout",
    type=DurationParam(),
    default="10m",
    help="Visibility timeout.",
    show_default=True,
)
@click.option(
    "--proc",
    "proc_cls",
    type=ProcessorParam(),
    help="Processor class to use.",
    default="shell",
)
@click.pass_context
async def pull(
    ctx: click.Context,
    visibility_timeout: timedelta,
    proc_cls: type[Processor],
) -> None:
    """Pull and execute a job"""
    # hours are set so they can just be used with user identity.
    # use worker for more fine-grained control.
    async with ctx.obj.get(exist_ok=True) as queue, proc_cls() as proc:  # noqa: SIM117
        async with queue.get_worker_interface() as worker_interface:
            try:
                await queue.pull_and_execute(
                    proc,
                    visibility_timeout=visibility_timeout,
                    worker_interface=worker_interface,
                )
            except WorkerCanceled:
                LOG.info("Worker canceled.")
            except EmptyQueue:
                LOG.error("Queue is empty.")
            except LockLostError:
                LOG.warning("Lock lost — task will be retried by another worker.")


@main.command("worker")
@click.option(
    "--visibility-timeout",
    type=DurationParam(),
    default="10m",
    help="Visibility timeout.",
    show_default=True,
)
@click.option(
    "--num-workers",
    "-n",
    type=int,
    default=1,
    help="Number of workers.",
    show_default=True,
)
@click.option(
    "--max-consecutive-failures",
    type=int,
    default=2,
    help="Maximum number of consecutive failures before exiting.",
    show_default=True,
)
@click.option(
    "--time-limit",
    type=DurationParam(),
    envvar="JOBQ_TIME_LIMIT",
    default="1d",
    help="Soft time limit. Tasks will receive SIGTERM when this is reached.",
    show_default=True,
)
@click.option(
    "--heartbeat/--no-heartbeat",
    default=True,
    help=(
        "Enable heartbeat for long running tasks, extending visibility_timeout indefinitely. "
        "Defaults to enabled."
    ),
)
@click.option(
    "--proc",
    "proc_cls",
    type=ProcessorParam(),
    help="Processor class to use.",
    default="shell",
)
@click.option(
    "--emulate-tty",
    "-t",
    is_flag=True,
    help="Emulate a TTY for the worker, forces line buffering (useful if logs get lost when preempted).",
)
@click.pass_context
async def workers(
    ctx: click.Context,
    visibility_timeout: timedelta,
    num_workers: int,
    time_limit: timedelta,
    max_consecutive_failures: int,
    heartbeat: bool,
    proc_cls: type[Processor],
    emulate_tty: bool = False,
) -> None:
    """Like pull, but start multiple async workers."""

    cfg: QueueConfig = ctx.obj
    assert cfg.log_handler is not None
    cfg.log_handler.plain_task_logs = num_workers < 2

    LOG.info(
        "JobQ %s starting %d workers with visibility timeout %s and time limit %s on PID %d.",
        __version__,
        num_workers,
        visibility_timeout,
        time_limit,
        os.getpid(),
    )

    async with AsyncExitStack() as stack:
        queue = await stack.enter_async_context(
            ctx.obj.get(exist_ok=True, require_account_key=False)
        )

        kwargs: dict[str, Any] = {}
        if emulate_tty:
            # Emulate a TTY for the worker, forces line buffering (useful if logs get lost when preempted).
            kwargs["emulate_tty"] = True

        async with proc_cls(num_workers=num_workers, **kwargs) as proc:  # type: ignore[call-arg]
            await launch_workers(
                queue,
                processor=proc,
                time_limit=time_limit,
                num_workers=num_workers,
                max_consecutive_failures=max_consecutive_failures,
                visibility_timeout=visibility_timeout,
                with_heartbeat=heartbeat,
                show_progress=sys.stdin.isatty(),  # Only in interactive sessions.
                environment_name=os.environ.get("JOBQ_ENVIRONMENT_NAME", ""),
            )


@main.command("size")
@click.pass_context
async def size(ctx: click.Context) -> None:
    """Get the approximate size of the queue."""
    async with ctx.obj.get(exist_ok=True) as queue:
        print(await queue.get_approximate_size())


@main.command("clear")
@click.pass_context
async def clear(ctx: click.Context) -> None:
    """Remove all jobs from the queue."""
    async with ctx.obj.get(exist_ok=True) as queue:
        await queue.clear()


class LAWorkspaceId(click.ParamType):
    name = "LogAnalytics-Workspace"

    def convert(self, value, param, ctx):
        try:
            import ai4s.jobq.la_workspace as la_tools
        except ImportError:
            _missing_track_deps()

        if value is None:
            if value := os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
                LOG.info("Using APPLICATIONINSIGHTS_CONNECTION_STRING from environment.")
            elif value := os.environ.get("APPLICATIONINSIGHTS_INSTRUMENTATIONKEY"):
                LOG.info("Using APPLICATIONINSIGHTS_INSTRUMENTATIONKEY from environment.")

        if value.startswith("/subscriptions/"):
            return la_tools.workspace_id_from_workspace_resource_id(value)
        if value.startswith("InstrumentationKey="):
            return la_tools.workspace_id_from_ikey(value.split("=")[1].split(";")[0])
        if la_tools.is_uid(value):
            try:
                # maybe it's an app insights key?
                return la_tools.workspace_id_from_ikey(value)
            except la_tools.WorkspaceNotFoundError:
                # then it's probably already the workspace name.
                return value
        return la_tools.workspace_id_from_ws_name(value)


def _missing_track_deps():
    LOG.error(
        "JobQ Track requires packages you don't have installed (eg. dash, dash_table, pandas, azure-monitor-query). "
        "Please install them with 'pip install ai4s-jobq[track]'."
    )

    raise SystemExit(1)


def _set_subscription_id(
    ctx: click.Context,
    param: click.Parameter,
    value: str | None,
) -> None:
    if value is not None:
        os.environ["JOBQ_AZURE_SUBSCRIPTION_ID"] = value


@main.command("track")
@click.argument(
    "log_analytics_workspace",
    type=LAWorkspaceId(),
    metavar="LOG_ANALYTICS_WORKSPACE",
    envvar="JOBQ_LA_WORKSPACE",
    required=True,
)
@click.option("port", "-p", default=8050, type=int, help="Port to run the dashboard on.")
@click.option(
    "--subscription-id",
    help="Azure Subscription ID to use. This is only needed when obtaining the workspace ID from an instrumentation key.",
    is_eager=True,
    expose_value=False,
    envvar="JOBQ_AZURE_SUBSCRIPTION_ID",
    callback=_set_subscription_id,
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
async def track(ctx: click.Context, log_analytics_workspace, debug, port) -> None:
    """
    Track the queue size and print it every 10 seconds.

    You can set JOBQ_LA_WORKSPACE_ID to the workspace ID of your Azure Log Analytics workspace.
    """

    workspace_id = log_analytics_workspace

    if not _jobq_track_requirements_are_available():
        _missing_track_deps()

    from ai4s.jobq.track.app import run_with_default_queue

    queue = ctx.obj.backend_spec

    os.environ["JOBQ_LA_WORKSPACE_ID"] = workspace_id

    run_with_default_queue(f"{queue!s}/{queue.name}", debug=debug, port=port)


def _jobq_track_requirements_are_available() -> bool:
    """Check if the requirements for jobq track are available."""
    try:
        import azure.monitor.query  # noqa: F401
        import dash  # noqa: F401
        import pandas  # noqa: F401

        return True
    except ImportError:
        return False


def _install_copilot_skill():  # pragma: no cover
    """Auto-install the Copilot skill if ~/.copilot/ exists and skill is outdated."""
    if "copilot-skill" in sys.argv:
        return
    if os.environ.get("JOBQ_COPILOT_SKILL", "1").lower() in ("0", "false", "no"):
        return
    skill_dir = os.path.join(os.path.expanduser("~"), ".copilot", "skills", "ai4s-jobq-cli")
    if not os.path.isdir(os.path.join(os.path.expanduser("~"), ".copilot")):
        return
    # Honor disabled marker left by 'ai4s-jobq copilot-skill clear'
    if os.path.exists(os.path.join(skill_dir, ".disabled")):
        return
    # Check version stamp to avoid re-installing on every invocation
    stamp = os.path.join(skill_dir, ".jobq-version")
    if os.path.exists(stamp):
        try:
            with open(stamp) as f:
                existing = f.read().strip()
            if existing == __version__:
                return
        except Exception:  # noqa: S110 — best-effort
            pass
    try:
        import shutil
        from importlib.resources import files as pkg_files

        bundled = pkg_files("ai4s.jobq.data.skill").joinpath("ai4s-jobq-cli")
        if bundled.is_dir() and bundled.joinpath("SKILL.md").is_file():
            if os.path.exists(skill_dir):
                shutil.rmtree(skill_dir)
            shutil.copytree(str(bundled), skill_dir)
            with open(stamp, "w") as f:
                f.write(__version__)
            LOG.info(
                "Auto-installed Copilot skill. "
                "Run 'ai4s-jobq copilot-skill clear' to disable auto-installation."
            )
    except Exception:  # noqa: S110 — best-effort
        pass


main.add_command(skill_file_cmd)
