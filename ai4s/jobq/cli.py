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
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import asyncclick as click
import yaml
from azure.core.credentials_async import AsyncTokenCredential

from ai4s.jobq import JobQ, __version__
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.entities import EmptyQueue, WorkerCanceled
from ai4s.jobq.logging_utils import JobQRichHandler, setup_logging
from ai4s.jobq.orchestration import WorkSpecification, batch_enqueue, get_results
from ai4s.jobq.orchestration.manager import launch_workers
from ai4s.jobq.work import DefaultSeed, ShellCommandProcessor

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


class BackendSpecParam(click.ParamType):
    def convert(
        self,
        value: Optional[Union[str, BackendSpec]],
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> Optional[BackendSpec]:
        if value is None:
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
        value: Optional[str],
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> Optional[Tuple[str, str]]:
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
        value: Optional[Union[timedelta, str]],
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> Optional[timedelta]:
        if value is None:
            return None

        if isinstance(value, timedelta):
            return value

        last = value[-1]
        if last in "smhdw":
            value = value[:-1]
            if last == "s":
                return timedelta(seconds=int(value))
            elif last == "m":
                return timedelta(minutes=int(value))
            elif last == "h":
                return timedelta(hours=int(value))
            elif last == "d":
                return timedelta(days=int(value))
            elif last == "w":
                return timedelta(weeks=int(value))
        return self.fail(f"Expected format: <number>[smhdw], got {value!r}", param, ctx)


class ProcessorParam(click.ParamType):
    def get_metavar(self, param: click.Parameter) -> str:
        return "[shell | map-in-config | <module>.<class>]"

    @staticmethod
    def get_class_from_string(qualified_name: str):
        # Split the fully qualified name into module and class parts
        parts = qualified_name.split(".")
        module_name = ".".join(parts[:-1])
        class_name = parts[-1]

        # Import the module dynamically
        module = importlib.import_module(module_name)

        # Get the class from the module
        cls = getattr(module, class_name)
        return cls

    def convert(
        self,
        value: Optional[str],
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> Optional[Type[ShellCommandProcessor]]:
        if value is None:
            return None
        if value == "shell":
            return ShellCommandProcessor
        elif value == "map-in-config":
            from ai4s.jobq.ext.map_in_config import MapInConfigProcessor

            return MapInConfigProcessor  # type: ignore
        else:
            return self.get_class_from_string(value)
        return self.fail(
            f"Expected one of 'shell' or 'map-in-config', got {value!r}",
            param,
            ctx,
        )


@dataclass
class QueueConfig:
    backend_spec: Optional[BackendSpec] = None
    conn_str: Optional[str] = None
    credential: Optional[Union[str, AsyncTokenCredential]] = None
    log_handler: Optional[JobQRichHandler] = None

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
                    credential: Optional[Union[str, AsyncTokenCredential]] = self.credential
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
                            credential=credential,  # type: ignore
                            exist_ok=exist_ok,
                        )
                    )
                elif isinstance(self.backend_spec, ServiceBusSpec):
                    credential = self.credential
                    if not self.credential:
                        credential = await stack.enter_async_context(get_token_credential())  # type: ignore
                    queue = await stack.enter_async_context(
                        JobQ.from_service_bus(
                            self.backend_spec.name,
                            fqns=f"{self.backend_spec.namespace}.servicebus.windows.net",
                            credential=credential,
                            exist_ok=exist_ok,
                        )
                    )
                else:
                    raise ValueError(f"Unknown backend spec: {self.backend_spec}")
            yield queue


@click.group()
@click.argument(
    "backend_spec",
    envvar="JOBQ_STORAGE_QUEUE",
    type=BackendSpecParam(),
    metavar="SERVICE/QUEUE_NAME",
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
    backend_spec: BackendSpec,
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

    ctx.ensure_object(QueueConfig)
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
@click.pass_context
async def push(
    ctx: click.Context,
    command: Iterable[str],
    num_retries: int,
    bg_dirsync_to: Optional[str],
    env_vars: List[Tuple[str, str]],
    wait: bool,
    num_enqueue_workers: int,
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

    class IteratorWorkSpec(WorkSpecification[Dict[str, Any], DefaultSeed]):
        async def list_tasks(
            self, seed: DefaultSeed, force: bool = False
        ) -> AsyncGenerator[Dict[str, Any], None]:
            for cmd in command:
                if cmd.startswith("{"):
                    job_spec = json.loads(cmd)
                    job_spec.setdefault("env", {}).update(env)
                    job_spec.setdefault("bg_dirsync_to", bg_dirsync_to)
                    yield job_spec
                else:
                    yield dict(cmd=cmd, bg_dirsync_to=bg_dirsync_to, env=env)

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
        raise SystemExit(1)


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
async def amlt_worker(ctx: click.Context, time_limit: timedelta, amlt_args: List[str]) -> None:
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
                    NamedTemporaryFile(
                        suffix=".yaml",
                        delete=False,
                        dir=os.path.dirname(amlt_args[i]),
                        mode="w",
                    )
                )
                with open(amlt_args[i], "r") as f:
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
    proc_cls: Type[ShellCommandProcessor],
) -> None:
    """Pull and execute a job"""
    # hours are set so they can just be used with user identity.
    # use worker for more fine-grained control.
    async with ctx.obj.get(exist_ok=True) as queue:
        async with proc_cls() as proc:
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
    proc_cls: Type[ShellCommandProcessor],
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

        kwargs: Dict[str, Any] = {}
        if emulate_tty:
            # Emulate a TTY for the worker, forces line buffering (useful if logs get lost when preempted).
            kwargs["emulate_tty"] = True

        async with proc_cls(num_workers=num_workers, **kwargs) as proc:
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
    value: Optional[str],
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
