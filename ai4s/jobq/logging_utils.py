# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import contextvars
import logging
import os
import sys
import traceback
from collections import deque
from typing import Any

from azure.core.credentials import TokenCredential
from cachetools import TTLCache
from opentelemetry._logs import get_logger_provider
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor
from opentelemetry.trace import SpanContext, TraceFlags
from rich.logging import RichHandler
from rich.text import Text

from .auth import get_sync_token_credential

TASK_LOG = logging.getLogger("task")
LOG = logging.getLogger("ai4s.jobq")
TRACK_LOG = logging.getLogger("ai4s.jobq.track")


def _azureml_run_description() -> dict[str, str]:
    """Returns a dict describing the AzureML run running the jobq worker."""
    if run_id := os.environ.get("AZUREML_RUN_ID"):
        workspace_scope = os.getenv("AZUREML_WORKSPACE_SCOPE")
        sub_id = os.environ.get("AZUREML_ARM_SUBSCRIPTION", "")
        resource_group = os.environ.get("AZUREML_ARM_RESOURCEGROUP", "")
        workspace_name = os.environ.get("AZUREML_ARM_WORKSPACE_NAME", "")
        return {
            "azureml_run_id": run_id,
            "azureml_workspace_name": workspace_name,
            "azureml_subscription_id": sub_id,
            "azureml_resource_group": resource_group,
            "azureml_project_name": os.environ.get("AZUREML_ARM_PROJECT_NAME", ""),
            "azureml_url": f"https://ml.azure.com/runs/{run_id}?wsid={workspace_scope}",
            "job_url": f"https://ml.azure.com/runs/{run_id}?wsid=/subscriptions/{sub_id}/resourcegroups/{resource_group}/workspaces/{workspace_name}",
        }
    else:
        # we are using the azureml_workspace_name in grafana dashboards to visualize queue status
        # which is why we set AZUREML_ARM_WORKSPACE_NAME also for e.g. azureml batch jobs
        return {
            "azureml_workspace_name": os.environ.get("AZUREML_ARM_WORKSPACE_NAME", ""),
            "job_url": os.environ.get("AZUREML_JOB_URL", ""),
        }


async def flush_app_insights():
    from ai4s.jobq.work import ProcessPoolRegistry

    await ProcessPoolRegistry.wait_for_msg_queue_to_drain()

    try:
        lp = get_logger_provider()
        if lp:
            if not getattr(lp, "force_flush", None):
                LOG.warning(
                    "Logger provider (%r) does not have force_flush() method: will not attempt to flush appinsights.",
                    lp.__class__.__name__,
                )
                return
            flushed = lp.force_flush()
            if not flushed:
                LOG.warning(
                    "force_flush() returned False: failed to flush application insights logs."
                )
            else:
                LOG.info("Flushed application insights logs.")
        else:
            LOG.warning("No logger provider found: cannot flush application insights logs.")
    except Exception as e:
        LOG.warning(f"Failed to flush application insights logs: {e}")


class SkipTaskLogsFilter(logging.Filter):
    """Filter out logs from task. modules."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith("task.")


_context_dimensions: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "_context_dimensions", default={}
)


class CustomDimensionsFilter(logging.Filter):
    """
    Adds custom 'extra' dimensions to log records (only if attribute is absent).
    Optionally drops records that carry exception context to prevent AppExceptions.
    """

    def __init__(self, custom_dimensions=None, *, filter_exceptions: bool = True):
        super().__init__()
        self.custom_dimensions = custom_dimensions or {}
        self.filter_exceptions = filter_exceptions

    def filter(self, record: logging.LogRecord) -> bool:
        if self.filter_exceptions:
            exc_info = getattr(record, "exc_info", None)
            if exc_info:
                exc_type, exc_value, exc_tb = exc_info

                # exceptions to traces to reduce AppExceptions table noise
                if exc_type:
                    tb_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
                    tb_string = "".join(tb_lines)

                    record.exception_type = exc_type.__name__
                    record.exception_message = str(exc_value)
                    record.exception_traceback = tb_string

                    # clear exc_info to prevent it from going to AppExceptions table
                    record.exc_info = None
                    record.exc_text = None

        # Start with static (process-wide) dimensions
        cdim = self.custom_dimensions.copy()
        # Layer on per-coroutine context dimensions (override static ones)
        cdim.update(_context_dimensions.get())
        for key, value in cdim.items():
            if getattr(record, key, None) is not None:
                continue
            setattr(record, key, value)
        return True


_custom_dimensions_filter: CustomDimensionsFilter | None = None
_pending_custom_dimensions: dict[str, str] = {}


def set_custom_dimensions(**kwargs: str) -> None:
    """Add process-wide custom dimensions that will be included in all log records.

    These are shared across all async tasks in the current process.
    For per-coroutine dimensions (e.g. a unique worker_id per async worker),
    use ``set_context_dimensions()`` instead.

    Can be called before ``setup_logging()``; dimensions will be buffered and
    applied once logging is initialised.
    """
    if _custom_dimensions_filter is not None:
        _custom_dimensions_filter.custom_dimensions.update(kwargs)
    else:
        _pending_custom_dimensions.update(kwargs)


def set_context_dimensions(**kwargs: str) -> None:
    """Set per-coroutine custom dimensions that will be included in log records.

    Uses ``contextvars`` so each ``asyncio.Task`` gets its own copy.
    Values set here override the process-wide dimensions from ``set_custom_dimensions()``.
    """
    ctx = _context_dimensions.get().copy()
    ctx.update(kwargs)
    _context_dimensions.set(ctx)


class CachingLogHandler(logging.Handler):
    def __init__(self, **kwargs: Any):
        self._caches: TTLCache[str, deque] = TTLCache(maxsize=100, ttl=60)

        super().__init__(**kwargs)

    def get_log_cache(self, task_id: str) -> deque:
        logger_name = f"task.{task_id}"
        if logger_name not in self._caches:
            return deque()
        return self._caches[logger_name]

    def emit(self, record: logging.LogRecord) -> None:
        # not emitting anything, just caching
        if record.name.startswith("task."):
            if record.name not in self._caches:
                self._caches[record.name] = deque(maxlen=100)
            self._caches[record.name].append(record.msg)


class JobQRichHandler(RichHandler):
    def __init__(self, plain_task_logs: bool = False, **kwargs: Any):
        self.plain_task_logs = plain_task_logs

        super().__init__(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.plain_task_logs and record.name.startswith("task."):
            # no formatting (prefix with logger=task id, timestamp), just the raw job output here.
            print(record.getMessage(), flush=True)
        else:
            super().emit(record)

    def get_level_text(self, record: logging.LogRecord) -> Text:
        from rich.text import Text

        level_name = record.levelname
        level_text = level_name[:1] * 2
        styled_text = Text.styled(level_text, f"logging.level.{level_name.lower()}")
        return styled_text


class PlainHandler(logging.StreamHandler):
    def __init__(self, plain_task_logs: bool = False, **kwargs: Any):
        self.plain_task_logs = plain_task_logs
        kwargs.setdefault("stream", sys.stdout)
        super().__init__(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.plain_task_logs and record.name.startswith("task."):
            # no formatting (prefix with logger=task id, timestamp), just the raw job output here.
            print(record.msg, flush=True)
        else:
            super().emit(record)


class SkipHttpProcessor(SpanProcessor):
    """
    SpanProcessor that filters out HTTP spans to prevent them from being exported.
    """

    def on_end(self, span: ReadableSpan) -> None:
        """
        Called when a span ends. If the span's component is HTTP, it prevents the span from being exported.

        Args:
            span (ReadableSpan): The span that has ended.
        """
        if span._attributes is None:
            return

        if span._attributes.get("component") == "http":
            self._do_not_export(span)

    @staticmethod
    def _do_not_export(span: ReadableSpan) -> None:
        """
        Modifies the span context to ensure the span is not exported.

        This method sets the `TraceFlags` of the span's context to `DEFAULT`, which means
        that the span will not be sampled and thus not exported. The `TraceFlags` is a bitmask that
        includes a sampling bit to indicate whether the span should be sampled (exported) or not.

        Args:
            span (ReadableSpan): The span to modify.
        """
        span._context = SpanContext(
            span.context.trace_id,
            span.context.span_id,
            span.context.is_remote,
            TraceFlags(TraceFlags.DEFAULT),  # Set the TraceFlags to DEFAULT to prevent exporting
            span.context.trace_state,
        )


def setup_logging(
    queue_spec: str,
    app_insights_connection_string: str | None = None,
    environment: str | None = None,
    internal_log_level: int = logging.INFO,
    base_log_level: int = logging.WARNING,
) -> logging.Handler:
    """
    Sets up logging for jobq. This handles a few special cases, which a simple logging.basicConfig() call would not:

    * if application insights connection string is provided, it will set up Azure Monitor integration
      (this can also be done by setting the environment variable APPLICATIONINSIGHTS_CONNECTION_STRING)

    * if the environment variable JOBQ_ENVIRONMENT_NAME is set, it will add it to the custom dimensions, so logs can
      e.g. be filtered by the cluster a job is running on

    * depending on whether the input is a terminal or not, it will use a different log format.

    Args:

        queue_spec (str): The queue specification, ends up in the custom dimensions of the logs.
        base_log_level (int): The log level for non-ai4s.jobq logs (default: logging.WARNING).
        internal_log_level (int): The log level for internal logs (default: logging.INFO).
    """

    cache_log_handler = CachingLogHandler()
    # Include timestamps in log messages only when not in interactive terminal
    if not sys.stdin.isatty():
        log_handler: PlainHandler | JobQRichHandler = PlainHandler(plain_task_logs=True)
        log_handler.setLevel(internal_log_level)
        fmt = "%(asctime)s %(levelname)s: %(message)s [%(name)s]"
    else:
        log_handler = JobQRichHandler(plain_task_logs=True, show_path=False, show_time=False)
        fmt = "%(name)s: %(message)s"

    logging.basicConfig(
        level=base_log_level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[cache_log_handler, log_handler],
    )

    connstr = app_insights_connection_string or os.environ.get(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )
    environment = environment or os.environ.get("JOBQ_ENVIRONMENT_NAME")

    global _custom_dimensions_filter
    custom_dims = {"queue": queue_spec} | _azureml_run_description()
    if environment:
        custom_dims["environment"] = environment
    custom_dims.update(_pending_custom_dimensions)
    _pending_custom_dimensions.clear()
    _custom_dimensions_filter = CustomDimensionsFilter(custom_dims)
    LOG.addFilter(_custom_dimensions_filter)
    TASK_LOG.addFilter(_custom_dimensions_filter)

    if connstr:
        try:
            from azure.monitor.opentelemetry import configure_azure_monitor

            try:
                credential: TokenCredential | None
                credential = get_sync_token_credential()
                # Use the Monitor scope for Application Insights with RBAC
                credential.get_token("https://monitor.azure.com/.default")
            except Exception as e:
                LOG.warning(
                    "Could not get a working token credential, setting up app insights without authentication"
                )
                LOG.debug(str(e))
                credential = None
        except ImportError:
            LOG.warning(
                "azure-monitor-opentelemetry cannot be imported. "
                "Please install it to enable Azure Monitor integration."
            )
        else:
            from opentelemetry.sdk._logs import LoggingHandler as _SdkLoggingHandler

            try:
                from opentelemetry.instrumentation.logging.handler import (
                    LoggingHandler as _InstrLoggingHandler,
                )

                _logging_handler_types: tuple[type, ...] = (
                    _SdkLoggingHandler,
                    _InstrLoggingHandler,
                )
            except ImportError:
                _logging_handler_types = (_SdkLoggingHandler,)

            configure_azure_monitor(
                connection_string=connstr,
                span_processors=[SkipHttpProcessor()],
                credential=credential,
                sampling_ratio=0.0,
                enable_performance_counters=False,
                enable_live_metrics=False,
            )

            # Suppress noisy tracebacks from the OTel exporter when the
            # AppInsights endpoint is unreachable (e.g. after preemption).
            # The exporter logs ERROR + full traceback on every failed
            # export attempt, which can drown out real user output.
            logging.getLogger("azure.monitor.opentelemetry.exporter.export._base").setLevel(
                logging.CRITICAL
            )

            # Find the Azure Monitor LoggingHandler by type instead of assuming position
            azure_handler = None
            for handler in logging.getLogger().handlers:
                if isinstance(handler, _logging_handler_types):
                    azure_handler = handler
                    break

            if azure_handler is not None:
                azure_handler.addFilter(SkipTaskLogsFilter())
                azure_handler.addFilter(_custom_dimensions_filter)
            else:
                LOG.warning("Azure Monitor handler not found, custom filters not applied")

    LOG.setLevel(internal_log_level)
    TRACK_LOG.setLevel(internal_log_level)
    TASK_LOG.setLevel(internal_log_level)

    return log_handler
