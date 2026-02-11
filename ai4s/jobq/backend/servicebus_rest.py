# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import json
import logging
import os
import time
import typing as ty
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType

import aiohttp
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import ResourceExistsError
from azure.servicebus.aio.management import ServiceBusAdministrationClient

from ai4s.jobq.entities import EmptyQueue, Response, Task

from .common import Envelope, JobQBackend

LOG = logging.getLogger(__name__)

SERVICE_BUS_SCOPE = "https://servicebus.azure.net/.default"


class _CachedTokenCredential:
    """Wraps an AsyncTokenCredential and caches the access token, refreshing 60s before expiry."""

    def __init__(self, credential: AsyncTokenCredential):
        self.credential = credential
        self.token: ty.Optional[AccessToken] = None

    async def __aenter__(self) -> "_CachedTokenCredential":
        await self.credential.__aenter__()
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        await self.credential.__aexit__(*args)

    async def get_token(self) -> str:
        if self.token is None or time.time() > self.token.expires_on - 60:
            self.token = await self.credential.get_token(SERVICE_BUS_SCOPE)
        return self.token.token


@dataclass
class _ReceivedMessage:
    """Parsed representation of a message received via the Service Bus REST API."""

    body: str
    message_id: str
    lock_token: str
    sequence_number: int
    delivery_count: int
    # The Location header from peek-lock contains the canonical URL for
    # completing/abandoning/renewing the message:
    #   https://{ns}/{queue}/messages/{sequenceNumber}/{lockToken}
    # Using this directly avoids 404s caused by wrong ID format.
    location_url: str
    # Lock duration in seconds, derived from LockedUntilUtc in BrokerProperties.
    lock_duration_seconds: float

    @staticmethod
    def from_response(resp: aiohttp.ClientResponse, body: str) -> "_ReceivedMessage":
        broker_props_raw = resp.headers.get("BrokerProperties", "{}")
        broker_props = json.loads(broker_props_raw)
        location = resp.headers.get("Location", "")

        # Compute lock duration from LockedUntilUtc if available.
        lock_duration = 30.0  # Service Bus default
        locked_until = broker_props.get("LockedUntilUtc")
        if locked_until:
            try:
                from datetime import datetime, timezone

                locked_until_dt = datetime.strptime(
                    locked_until, "%a, %d %b %Y %H:%M:%S %Z"
                ).replace(tzinfo=timezone.utc)
                lock_duration = max(
                    (locked_until_dt - datetime.now(timezone.utc)).total_seconds(), 10.0
                )
            except (ValueError, TypeError):
                LOG.debug("Could not parse LockedUntilUtc: %s", locked_until)

        return _ReceivedMessage(
            body=body,
            message_id=broker_props.get("MessageId", ""),
            lock_token=broker_props.get("LockToken", ""),
            sequence_number=broker_props.get("SequenceNumber", 0),
            delivery_count=broker_props.get("DeliveryCount", 0),
            location_url=location,
            lock_duration_seconds=lock_duration,
        )


class RESTServiceBusEnvelope(Envelope):
    def __init__(
        self,
        message: _ReceivedMessage,
        task: Task,
        client: "RESTServiceBusClient",
        lock_renewal_task: ty.Optional[asyncio.Task[None]] = None,
        lock_stop_event: ty.Optional[asyncio.Event] = None,
    ):
        self.message = message
        self._task = task
        self._client = client
        self._lock_renewal_task = lock_renewal_task
        self._lock_stop_event = lock_stop_event
        self.done = False

    @property
    def id(self) -> str:
        return self.message.message_id

    @property
    def task(self) -> Task:
        return self._task

    async def cancel_heartbeat(self) -> None:
        if self._lock_stop_event is not None:
            self._lock_stop_event.set()
        if self._lock_renewal_task is not None and not self._lock_renewal_task.done():
            with suppress(asyncio.CancelledError):
                await self._lock_renewal_task
            self._lock_renewal_task = None

    async def requeue(self) -> None:
        LOG.debug(f"Requeueing message {self.id}")
        await self.cancel_heartbeat()
        await self._client._unlock_message(self.message.location_url)

    async def reply(self, response: Response) -> None:
        raise NotImplementedError("REST ServiceBus backend does not support replies yet.")

    async def delete(self, success: bool, error: str | None = None) -> None:
        await self.cancel_heartbeat()
        if success:
            await self._client._complete_message(self.message.location_url)
        else:
            await self._client._deadletter_message(
                self.message.location_url,
                reason=error or "Failed",
            )
        self.done = True

    async def abandon(self) -> None:
        await self.cancel_heartbeat()
        await self._client._unlock_message(self.message.location_url)
        self.done = True

    async def replace(self, task: Task) -> None:
        await self._client._send_message(task.serialize())


class RESTServiceBusClient(JobQBackend):
    """Service Bus backend that uses pure REST/HTTP calls via aiohttp instead of AMQP."""

    def __init__(
        self,
        queue_name: str,
        *,
        fqns: ty.Optional[str] = None,
        credential: ty.Optional[ty.Any] = None,
        exist_ok: bool = True,
    ):
        self.fqns = fqns
        self.queue_name = queue_name
        self.reply_queue_name = queue_name + "-replies"
        self.credential = credential
        self._exist_ok = exist_ok
        self._session: ty.Optional[aiohttp.ClientSession] = None
        self._cached_credential: ty.Optional[_CachedTokenCredential] = None

    @property
    def _base_url(self) -> str:
        return f"https://{self.fqns}"

    async def __aenter__(self) -> "RESTServiceBusClient":
        if self.credential is None:
            raise RuntimeError("No credential provided.")
        assert self.fqns is not None

        if isinstance(self.credential, AsyncTokenCredential):
            self._cached_credential = _CachedTokenCredential(self.credential)
            await self._cached_credential.__aenter__()

        self._session = aiohttp.ClientSession()
        await self.create(exist_ok=self._exist_ok)
        return self

    async def __aexit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._cached_credential is not None:
            await self._cached_credential.__aexit__(exc_type, exc, tb)
            self._cached_credential = None

    async def _auth_headers(self) -> dict[str, str]:
        assert self._cached_credential is not None, "Credential not initialized."
        token = await self._cached_credential.get_token()
        return {"Authorization": f"Bearer {token}"}

    # ── REST data-plane helpers ──────────────────────────────────────────

    async def _send_message(self, body: str, broker_properties: ty.Optional[dict] = None) -> str:
        """Send a message to the queue. Returns the message ID."""
        assert self._session is not None
        url = f"{self._base_url}/{self.queue_name}/messages"
        headers = await self._auth_headers()
        headers["Content-Type"] = "application/atom+xml;type=entry;charset=utf-8"
        message_id = uuid.uuid4().hex
        bp: dict[str, ty.Any] = {"MessageId": message_id}
        if broker_properties:
            bp.update(broker_properties)
        headers["BrokerProperties"] = json.dumps(bp)

        async with self._session.post(url, data=body, headers=headers) as resp:
            resp.raise_for_status()
        return message_id

    async def _peek_lock_message(self, timeout: int = 60) -> _ReceivedMessage:
        """Receive one message with peek-lock via REST."""
        assert self._session is not None
        url = f"{self._base_url}/{self.queue_name}/messages/head"
        headers = await self._auth_headers()
        params = {"timeout": str(timeout)}

        async with self._session.post(url, headers=headers, params=params) as resp:
            if resp.status in (204, 404):
                raise EmptyQueue(f"The queue {self.name} has no more tasks.")
            resp.raise_for_status()
            body = await resp.text()
            return _ReceivedMessage.from_response(resp, body)

    async def _receive_and_delete_message(self, timeout: int = 2) -> ty.Optional[str]:
        """Receive and immediately delete one message (destructive read)."""
        assert self._session is not None
        url = f"{self._base_url}/{self.queue_name}/messages/head"
        headers = await self._auth_headers()
        params = {"timeout": str(timeout)}

        async with self._session.delete(url, headers=headers, params=params) as resp:
            if resp.status in (204, 404):
                return None
            resp.raise_for_status()
            return await resp.text()

    async def _complete_message(self, location_url: str) -> None:
        """Complete (delete) a previously peek-locked message using the Location URL."""
        assert self._session is not None
        headers = await self._auth_headers()

        async with self._session.delete(location_url, headers=headers) as resp:
            resp.raise_for_status()

    async def _unlock_message(self, location_url: str) -> None:
        """Abandon (unlock) a previously peek-locked message, making it available again."""
        assert self._session is not None
        headers = await self._auth_headers()

        async with self._session.put(location_url, headers=headers) as resp:
            resp.raise_for_status()

    async def _deadletter_message(self, location_url: str, reason: str = "Failed") -> None:
        """Move a peek-locked message to the dead-letter sub-queue."""
        assert self._session is not None
        headers = await self._auth_headers()
        headers["Content-Type"] = "application/atom+xml;type=entry;charset=utf-8"
        body = json.dumps({"DispositionStatus": "Defered", "DeadLetterReason": reason})
        # The REST API uses a PUT with DeadLetterReason to dead-letter.
        # We set DispositionStatus to signal the intent; azure interprets the
        # presence of DeadLetterReason on the PUT as a dead-letter operation.
        async with self._session.put(location_url, headers=headers, data=body) as resp:
            resp.raise_for_status()

    async def _renew_lock(self, location_url: str) -> None:
        """Renew the lock on a peek-locked message using the Location URL."""
        assert self._session is not None
        headers = await self._auth_headers()

        async with self._session.post(location_url, headers=headers) as resp:
            resp.raise_for_status()

    async def _peek_messages(self, n: int = 1) -> ty.List[dict]:
        """Non-destructive peek at messages (no lock acquired)."""
        assert self._session is not None
        messages: ty.List[dict] = []
        headers = await self._auth_headers()

        for _ in range(n):
            url = f"{self._base_url}/{self.queue_name}/messages/head"
            params = {"peekonly": "true"}
            async with self._session.post(url, headers=headers, params=params) as resp:
                if resp.status == 204:
                    break
                resp.raise_for_status()
                body = await resp.text()
                broker_props = json.loads(resp.headers.get("BrokerProperties", "{}"))
                messages.append({"body": body, "broker_properties": broker_props})

        return messages

    # ── JobQBackend protocol implementation ──────────────────────────────

    async def push(self, task: Task) -> str:
        raise RuntimeError("Use a RESTServiceBusWorker to push messages to service bus")

    async def get_result(self, session_id: str, timeout: ty.Optional[timedelta] = None) -> Response:
        raise NotImplementedError("REST ServiceBus backend does not support get_result yet.")

    @asynccontextmanager  # type: ignore
    async def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs: ty.Any
    ) -> ty.AsyncGenerator[RESTServiceBusEnvelope, None]:
        raise RuntimeError("Use RESTServiceBusWorker to receive messages.")
        yield  # unreachable, but required for generator typing

    async def create(self, exist_ok: bool = True) -> None:
        async with self._get_admin_client() as admin_client:
            try:
                await admin_client.create_queue(queue_name=self.queue_name, requires_session=False)
                LOG.info(f"Created queue {self.queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise
            try:
                await admin_client.create_queue(
                    queue_name=self.reply_queue_name, requires_session=True
                )
                LOG.info(f"Created queue {self.reply_queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise

    async def clear(self) -> None:
        """Drain the queue via destructive REST reads."""
        n = 0
        max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 2))
        while True:
            body = await self._receive_and_delete_message(timeout=max_wait_time)
            if body is None:
                break
            n += 1
        LOG.info(f"Cleared {n} messages from queue {self.queue_name}.")

    @asynccontextmanager
    async def _get_admin_client(self) -> ty.AsyncGenerator[ServiceBusAdministrationClient, None]:
        if self.credential is not None:
            assert self.fqns is not None, "Fully qualified namespace for service bus is required."
            async with ServiceBusAdministrationClient(
                fully_qualified_namespace=self.fqns,
                credential=self.credential,  # type: ignore
            ) as aclt:
                yield aclt
        else:
            raise RuntimeError("No credential provided.")

    async def __len__(self) -> int:
        async with self._get_admin_client() as aclt:
            queue_runtime_info = await aclt.get_queue_runtime_properties(queue_name=self.queue_name)
            ret = queue_runtime_info.active_message_count
            assert isinstance(ret, int)
            return ret

    @property
    def name(self) -> str:
        return f"{self.fqns}/{self.queue_name}"

    def generate_sas(self, ttl: timedelta) -> str:
        raise NotImplementedError("REST ServiceBus does not yet support SAS tokens.")

    async def peek(self, n: int = 1, as_json: bool = False) -> ty.List[ty.Any]:
        messages = await self._peek_messages(n)
        if not messages:
            raise EmptyQueue(f"The queue {self.name} has no more tasks.")
        if as_json:
            return [json.loads(m["body"]) for m in messages]
        return messages

    @asynccontextmanager
    async def get_worker_interface(
        self,
        receiver_kwargs: ty.Optional[dict] = None,
        sender_kwargs: ty.Optional[dict] = None,
        no_receiver: bool = False,
        no_sender: bool = False,
        **kwargs: ty.Any,
    ) -> ty.AsyncGenerator["RESTServiceBusWorker", None]:  # type: ignore
        worker = RESTServiceBusWorker(
            self,
            no_receiver=no_receiver,
            no_sender=no_sender,
        )
        await worker.__aenter__()
        try:
            yield worker
        finally:
            await worker.__aexit__(None, None, None)


class RESTServiceBusWorker:
    """Worker that sends/receives Service Bus messages via REST."""

    def __init__(
        self,
        backend: RESTServiceBusClient,
        no_receiver: bool = False,
        no_sender: bool = False,
    ):
        self.backend = backend
        self._no_receiver = no_receiver
        self._no_sender = no_sender
        self._max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 5))
        # max lock renewal lifetime: 3 weeks (same as AMQP backend)
        self._max_lock_renewal_seconds = 60 * 60 * 24 * 21

    async def __aenter__(self) -> "RESTServiceBusWorker":
        return self

    async def __aexit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None:
        pass

    def _start_lock_renewal(
        self, message: _ReceivedMessage, interval: float
    ) -> tuple[asyncio.Task[None], asyncio.Event]:
        """Start a background task that periodically renews the message lock.

        Returns the task and a stop event that can be set to gracefully stop renewal.
        """
        deadline = time.monotonic() + self._max_lock_renewal_seconds
        stop_event = asyncio.Event()

        async def _renew_loop() -> None:
            try:
                while time.monotonic() < deadline and not stop_event.is_set():
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    if stop_event.is_set():
                        return
                    try:
                        await self.backend._renew_lock(message.location_url)
                        LOG.debug("Renewed lock for message %s", message.message_id)
                    except asyncio.CancelledError:
                        raise
                    except aiohttp.ClientResponseError as exc:
                        if exc.status == 404:
                            # Message was already settled (completed/dead-lettered).
                            LOG.debug(
                                "Lock renewal stopped for message %s: message already settled.",
                                message.message_id,
                            )
                            return
                        LOG.warning(
                            "Failed to renew lock for message %s: %s",
                            message.message_id,
                            exc,
                        )
                    except Exception:
                        LOG.warning(
                            "Failed to renew lock for message %s",
                            message.message_id,
                            exc_info=True,
                        )
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_renew_loop(), name=f"lock-renew-{message.message_id}")
        return task, stop_event

    @asynccontextmanager
    async def receive_message(
        self,
        visibility_timeout: timedelta,
        with_heartbeat: bool = False,
        **kwargs: ty.Any,
    ) -> ty.AsyncGenerator[RESTServiceBusEnvelope, None]:
        assert not self._no_receiver, "Receiver is disabled."

        # Retry several times before declaring the queue empty.
        # Unlike AMQP (persistent link), each REST call is independent and may
        # miss messages that are temporarily locked by other receivers.
        max_empty_polls = int(os.environ.get("JOBQ_SERVICEBUS_EMPTY_POLLS", 3))
        for attempt in range(max_empty_polls):
            try:
                message = await self.backend._peek_lock_message(timeout=self._max_wait_time)
                break
            except EmptyQueue:
                if attempt == max_empty_polls - 1:
                    raise
                LOG.debug("No messages on attempt %d/%d, retrying…", attempt + 1, max_empty_polls)
                await asyncio.sleep(1)
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt == max_empty_polls - 1:
                    raise EmptyQueue(f"The queue {self.backend.name} is unreachable: {exc}")
                LOG.debug("Transient error on attempt %d/%d: %s", attempt + 1, max_empty_polls, exc)
                await asyncio.sleep(1)

        lock_task: ty.Optional[asyncio.Task[None]] = None
        lock_stop_event: ty.Optional[asyncio.Event] = None
        if with_heartbeat:
            # Renew at half the actual lock duration reported by Service Bus,
            # NOT the application-level visibility_timeout which can be hours.
            interval = max(message.lock_duration_seconds / 2, 5)
            lock_task, lock_stop_event = self._start_lock_renewal(message, interval)

        try:
            task = Task.deserialize(message.body)
        except Exception:
            LOG.error(
                "Stopping processing due to deserialization error to prevent potential data loss.",
                exc_info=True,
            )
            raise
        else:
            envelope = RESTServiceBusEnvelope(
                message, task, self.backend, lock_task, lock_stop_event
            )
            try:
                yield envelope
            finally:
                if lock_stop_event is not None:
                    lock_stop_event.set()
                if lock_task is not None and not lock_task.done():
                    with suppress(asyncio.CancelledError):
                        await lock_task

    async def push(self, task: Task) -> str:
        assert not self._no_sender, "Sender is disabled."
        return await self.backend._send_message(task.serialize())
