# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ruff: noqa: I001
from importlib.metadata import version

from .entities import EmptyQueue, LockLostError, PermanentTaskFailure, Response, WorkerCanceled
from .jobq import JobQ, JobQFuture
from .work import ProcessPool, Processor, WorkSpecification
from .orchestration.manager import batch_enqueue, launch_workers
from .logging_utils import setup_logging


__version__ = version("ai4s.jobq")


__all__ = [
    "EmptyQueue",
    "JobQ",
    "JobQFuture",
    "LockLostError",
    "PermanentTaskFailure",
    "ProcessPool",
    "Processor",
    "Response",
    "WorkSpecification",
    "WorkerCanceled",
    "batch_enqueue",
    "launch_workers",
    "setup_logging",
]
