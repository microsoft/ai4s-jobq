# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import json
import os
import uuid
from dataclasses import dataclass, field
from hashlib import md5
from typing import Any, Dict, Optional

if os.getenv("JOBQ_USE_MONTY_JSON", "").lower() in ("1", "true", "yes"):
    from monty.json import MontyDecoder, MontyEncoder  # type: ignore

    JSON_ENCODER = MontyEncoder  # type: ignore
    JSON_DECODER = MontyDecoder  # type: ignore
else:
    JSON_ENCODER = json.JSONEncoder
    JSON_DECODER = json.JSONDecoder


JOBQ_DETERMINISTIC_IDS = os.getenv("JOBQ_DETERMINISTIC_IDS", "").lower() in (
    "1",
    "true",
    "yes",
)


class EmptyQueue(Exception):
    """Raised when a queue is empty."""

    pass


class WorkerCanceled(Exception):
    """Raised when a worker is canceled."""

    pass


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
    kwargs: Dict[str, Any]
    num_retries: int
    error: Optional[str] = None
    reply_requested: bool = False
    id: Optional[str] = field(
        default_factory=lambda: uuid.uuid4().hex if not JOBQ_DETERMINISTIC_IDS else None
    )

    @property
    def _id(self):
        if self.id:
            return self.id
        return md5(json.dumps(self._dict_without_id(), cls=JSON_ENCODER)).hexdigest()

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
        if data["version"] == 1:
            return Task(
                id=data["id"],
                kwargs=json.loads(data["kwargs"], cls=JSON_DECODER),
                num_retries=data["num_retries"],
                reply_requested=data["reply_requested"],
            )
        elif data["version"] == 0:
            return Task(
                id=data["id"],
                kwargs=json.loads(data["kwargs"], cls=JSON_DECODER),
                num_retries=data["num_retries"],
                reply_requested=False,
            )
        else:
            raise ValueError("Unsupported Task version {}".format(data["version"]))
