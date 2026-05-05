# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import json
import os
import uuid
from dataclasses import dataclass, field
from hashlib import md5
from typing import Any

if os.getenv("JOBQ_USE_MONTY_JSON", "").lower() in ("1", "true", "yes"):
    from monty.json import MontyDecoder, MontyEncoder

    JSON_ENCODER = MontyEncoder
    JSON_DECODER = MontyDecoder
else:
    JSON_ENCODER = json.JSONEncoder
    JSON_DECODER = json.JSONDecoder


JOBQ_DETERMINISTIC_IDS = os.getenv("JOBQ_DETERMINISTIC_IDS", "true").lower() in (
    "1",
    "true",
    "yes",
)


class EmptyQueue(Exception):  # noqa: N818 — public API name
    """Raised when a queue is empty."""


class WorkerCanceled(Exception):  # noqa: N818 — public API name
    """Raised when a worker is canceled."""


class LockLostError(Exception):
    """Raised when the message lock is lost during task execution.

    This is an infrastructure event (for example, the lock expired or another
    client completed the message) — **not** a task failure.  Callers should
    handle this separately from task failures so that it does not count toward
    consecutive-failure thresholds.
    """


class PermanentTaskFailure(Exception):  # noqa: N818 — public API name
    """Raised by task callbacks to signal a non-retryable failure.

    When the consumer loop sees this exception it skips the remaining retry
    budget and dead-letters the message immediately.  Use it for failures
    that are deterministic with respect to the task payload (for example,
    a payload routed to the wrong queue, schema violations, or unsupported
    workload configurations) where retrying would only burn worker time.
    """


@dataclass
class Response:
    is_success: bool
    body: Any

    def serialize(self) -> str:
        return json.dumps(
            {
                "version": 1,
                "is_success": self.is_success,
                "body": json.dumps(self.body),
            }
        )

    @staticmethod
    def deserialize(string: str) -> "Response":
        data = json.loads(string)
        assert data["version"] == 1, f"Unsupported version of Response data: {data!r}"
        return Response(is_success=data["is_success"], body=json.loads(data["body"]))


@dataclass(frozen=True)
class Task:
    kwargs: dict[str, Any]
    num_retries: int
    error: str | None = None
    reply_requested: bool = False
    id: str | None = field(
        default_factory=lambda: uuid.uuid4().hex if not JOBQ_DETERMINISTIC_IDS else None
    )

    @property
    def _id(self):
        if self.id:
            return self.id
        return md5(json.dumps(self._dict_without_id(), cls=JSON_ENCODER).encode()).hexdigest()

    def _dict_without_id(self):
        return {
            "version": 1,
            "reply_requested": self.reply_requested,
            "kwargs": json.dumps(self.kwargs, cls=JSON_ENCODER),
            "num_retries": self.num_retries,
        }

    def serialize(self) -> str:
        res = self._dict_without_id().copy()
        res["id"] = self._id
        return json.dumps(res)

    @staticmethod
    def deserialize(string: str) -> "Task":
        data = json.loads(string)
        if "version" not in data:
            # Legacy format written by one-shot tooling (e.g. replay scripts)
            # that hand-crafted the envelope without version/id fields.
            # Treat as v1 with a deterministically computed id.
            return Task(
                id=None,
                kwargs=json.loads(data["kwargs"], cls=JSON_DECODER),
                num_retries=data.get("num_retries", 5),
                reply_requested=data.get("reply_requested", False),
            )
        if data["version"] == 1:
            return Task(
                id=data["id"],
                kwargs=json.loads(data["kwargs"], cls=JSON_DECODER),
                num_retries=data["num_retries"],
                reply_requested=data["reply_requested"],
            )
        if data["version"] == 0:
            return Task(
                id=data["id"],
                kwargs=json.loads(data["kwargs"], cls=JSON_DECODER),
                num_retries=data["num_retries"],
                reply_requested=False,
            )
        raise ValueError("Unsupported Task version {}".format(data["version"]))
