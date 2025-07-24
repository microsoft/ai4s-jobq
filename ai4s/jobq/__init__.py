# ruff: noqa: I001
MOLDYN_SUBSCRIPTION_ID = "3eaeebff-de6e-4e20-9473-24de9ca067dc"

from importlib.metadata import version  # noqa: E402

from .entities import EmptyQueue, Response, WorkerCanceled  # noqa: E402
from .jobq import JobQ, JobQFuture  # noqa: E402
from .work import ProcessPool, WorkSpecification  # noqa: E402
from .orchestration.manager import batch_enqueue, launch_workers  # noqa: E402
from .logging_utils import setup_logging  # noqa: E402

__version__ = version("ai4s.jobq")


__all__ = [
    "JobQ",
    "JobQFuture",
    "EmptyQueue",
    "WorkerCanceled",
    "Response",
    "WorkSpecification",
    "ProcessPool",
    "batch_enqueue",
    "launch_workers",
    "setup_logging",
]
