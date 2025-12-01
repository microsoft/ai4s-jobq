# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
import os
import sys
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
        return {
            "azureml_run_id": run_id,
            "azureml_workspace_name": os.environ.get("AZUREML_ARM_WORKSPACE_NAME", ""),
            "azureml_subscription_id": os.environ.get("AZUREML_ARM_SUBSCRIPTION", ""),
            "azureml_resource_group": os.environ.get("AZUREML_ARM_RESOURCEGROUP", ""),
            "azureml_project_name": os.environ.get("AZUREML_ARM_PROJECT_NAME", ""),
        }
    else:
        return {}


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


class CustomDimensionsFilter(logging.Filter):
    def __init__(self, custom_dimensions=None):
        self.custom_dimensions = custom_dimensions or {}

    def filter(self, record):
        cdim = self.custom_dimensions.copy()
        for key, value in cdim.items():
            if getattr(record, key, None) is not None:
                continue
            setattr(record, key, value)
        return True


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
        fmt = "%(levelname).1s%(levelname).1s %(asctime)s\t%(name)s: %(message)s"
    else:
        log_handler = JobQRichHandler(plain_task_logs=True, show_path=False, show_time=False)
        fmt = "%(name)s: %(message)s"

    logging.basicConfig(
        level=base_log_level,
        format=fmt,
        datefmt="[%X]",
        handlers=[cache_log_handler, log_handler],
    )

    connstr = app_insights_connection_string or os.environ.get(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )
    environment = environment or os.environ.get("JOBQ_ENVIRONMENT_NAME")
    if connstr:
        try:
            from azure.monitor.opentelemetry import configure_azure_monitor

            try:
                credential: TokenCredential | None
                credential = get_sync_token_credential()
                credential.get_token("https://management.azure.com/.default")
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
            configure_azure_monitor(
                connection_string=connstr,
                span_processors=[SkipHttpProcessor()],
                credential=credential,
            )
            azure_handler = logging.getLogger().handlers[-1]
            azure_handler.addFilter(SkipTaskLogsFilter())

            custom_dims = {"queue": queue_spec} | _azureml_run_description()
            if environment:
                custom_dims["environment"] = environment
            flt = CustomDimensionsFilter(custom_dims)
            LOG.addFilter(flt)
            TASK_LOG.addFilter(flt)
            azure_handler.addFilter(flt)

    LOG.setLevel(internal_log_level)
    TRACK_LOG.setLevel(internal_log_level)
    TASK_LOG.setLevel(internal_log_level)

    return log_handler
