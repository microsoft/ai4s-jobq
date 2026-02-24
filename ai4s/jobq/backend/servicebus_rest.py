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
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ai4s.jobq.entities import EmptyQueue, Response, Task

from .common import Envelope, JobQBackend

LOG = logging.getLogger(__name__)

SERVICE_BUS_SCOPE = "https://servicebus.azure.net/.default"

_RETRYABLE_STATUS_CODES = frozenset({401, 408, 429, 500, 502, 503, 504})


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


def _parse_lock_duration(broker_props: dict) -> float:
    """Extract lock duration in seconds from BrokerProperties, falling back to 30s default."""
    lock_duration = 30.0  # Service Bus default
    locked_until = broker_props.get("LockedUntilUtc")
    if locked_until:
        try:
            from datetime import datetime, timezone

            locked_until_dt = datetime.strptime(locked_until, "%a, %d %b %Y %H:%M:%S %Z").replace(
                tzinfo=timezone.utc
            )
            lock_duration = max(
                (locked_until_dt - datetime.now(timezone.utc)).total_seconds(), 10.0
            )
        except (ValueError, TypeError):
            LOG.debug("Could not parse LockedUntilUtc: %s", locked_until)
    return lock_duration


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient errors that should be retried."""
    if isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError, TimeoutError)):
        return True
    if isinstance(exc, aiohttp.ClientResponseError) and exc.status in _RETRYABLE_STATUS_CODES:
        return True
    return False


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

        lock_duration = _parse_lock_duration(broker_props)

        return _ReceivedMessage(
            body=body,
            message_id=broker_props.get("MessageId", ""),
            lock_token=broker_props.get("LockToken", ""),
            sequence_number=broker_props.get("SequenceNumber", 0),
            delivery_count=broker_props.get("DeliveryCount", 0),
            location_url=location,
            lock_duration_seconds=lock_duration,
        )


# ── Low-level REST client ───────────────────────────────────────────────


class ServiceBusRestClient:
    """Low-level Service Bus REST/HTTP client that wraps aiohttp.

    Handles authentication, session management, and the individual data-plane
    operations (send, peek-lock, complete, unlock, dead-letter, renew, peek).

    All HTTP calls are retried on transient network errors and throttling
    (429, 500, 502, 503, 504) with exponential backoff and jitter.
    On 401 responses the token is refreshed and the request retried once.
    """

    def __init__(
        self,
        fqns: str,
        queue_name: str,
        credential: AsyncTokenCredential,
    ):
        self.fqns = fqns
        self.queue_name = queue_name
        self._credential = credential
        self._session: ty.Optional[aiohttp.ClientSession] = None
        self._cached_credential: ty.Optional[_CachedTokenCredential] = None
        self._max_retries = int(os.environ.get("JOBQ_SERVICEBUS_MAX_RETRIES", 4))

    @property
    def _base_url(self) -> str:
        return f"https://{self.fqns}"

    async def __aenter__(self) -> "ServiceBusRestClient":
        self._cached_credential = _CachedTokenCredential(self._credential)
        await self._cached_credential.__aenter__()
        connector = aiohttp.TCPConnector(
            keepalive_timeout=30,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
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

    async def _force_token_refresh(self) -> None:
        """Invalidate the cached token so the next call fetches a fresh one."""
        assert self._cached_credential is not None
        self._cached_credential.token = None

    async def _request(
        self,
        method: str,
        url: str,
        *,
        headers: ty.Optional[dict[str, str]] = None,
        data: ty.Optional[str] = None,
        params: ty.Optional[dict[str, str]] = None,
        timeout: ty.Optional[aiohttp.ClientTimeout] = None,
        max_retries: ty.Optional[int] = None,
    ) -> aiohttp.ClientResponse:
        """Execute an HTTP request with retry and 401 token-refresh logic.

        Returns the *already-read* response (body consumed). Callers should
        access ``resp.status``, ``resp.headers``, and ``resp._body`` / the
        returned text via the helper methods that call this.
        """
        assert self._session is not None
        effective_retries = max_retries if max_retries is not None else self._max_retries

        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(effective_retries),
            wait=wait_exponential_jitter(initial=0.5, max=10, jitter=1),
            before_sleep=lambda rs: LOG.warning(
                "Retrying %s %s (attempt %d/%d) after %s",
                method,
                url,
                rs.attempt_number,
                effective_retries,
                rs.outcome.exception(),
            ),
            reraise=True,
        )
        async def _do_request() -> aiohttp.ClientResponse:
            assert self._session is not None
            req_headers = await self._auth_headers()
            if headers:
                req_headers.update(headers)

            resp = await self._session.request(
                method,
                url,
                headers=req_headers,
                data=data,
                params=params,
                timeout=timeout,
            )
            # On 401, force token refresh and raise to trigger retry
            if resp.status == 401:
                resp.close()
                await self._force_token_refresh()
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=401,
                    message="Unauthorized — token refreshed, retrying",
                )
            return resp

        return await _do_request()

    async def send_message(
        self,
        body: str,
        broker_properties: ty.Optional[dict] = None,
        message_id: ty.Optional[str] = None,
    ) -> str:
        """Send a message to the queue. Returns the message ID."""
        url = f"{self._base_url}/{self.queue_name}/messages"
        # Generate MessageId once so retries are idempotent
        message_id = message_id or uuid.uuid4().hex
        bp: dict[str, ty.Any] = {"MessageId": message_id}
        if broker_properties:
            bp.update(broker_properties)
        extra_headers = {
            "Content-Type": "application/atom+xml;type=entry;charset=utf-8",
            "BrokerProperties": json.dumps(bp),
        }

        resp = await self._request("POST", url, headers=extra_headers, data=body)
        resp.close()
        return message_id

    async def peek_lock_message(self, timeout: int = 60) -> _ReceivedMessage:
        """Receive one message with peek-lock via REST."""
        url = f"{self._base_url}/{self.queue_name}/messages/head"
        params = {"timeout": str(timeout)}

        resp = await self._request("POST", url, params=params)
        try:
            if resp.status in (204, 404):
                raise EmptyQueue(f"The queue {self.fqns}/{self.queue_name} has no more tasks.")
            resp.raise_for_status()
            body = await resp.text()
            return _ReceivedMessage.from_response(resp, body)
        finally:
            resp.close()

    async def receive_and_delete_message(self, timeout: int = 2) -> ty.Optional[str]:
        """Receive and immediately delete one message (destructive read)."""
        url = f"{self._base_url}/{self.queue_name}/messages/head"
        params = {"timeout": str(timeout)}

        resp = await self._request("DELETE", url, params=params)
        try:
            if resp.status in (204, 404):
                return None
            resp.raise_for_status()
            return await resp.text()
        finally:
            resp.close()

    async def complete_message(self, location_url: str) -> None:
        """Complete (delete) a previously peek-locked message using the Location URL."""
        resp = await self._request("DELETE", location_url)
        resp.raise_for_status()
        resp.close()

    async def unlock_message(self, location_url: str) -> None:
        """Abandon (unlock) a previously peek-locked message, making it available again."""
        resp = await self._request("PUT", location_url)
        resp.raise_for_status()
        resp.close()

    async def deadletter_message(self, location_url: str, reason: str = "Failed") -> None:
        """Move a peek-locked message to the dead-letter sub-queue."""
        extra_headers = {
            "Content-Type": "application/atom+xml;type=entry;charset=utf-8",
        }
        body = json.dumps({"DispositionStatus": "Defered", "DeadLetterReason": reason})
        resp = await self._request("PUT", location_url, headers=extra_headers, data=body)
        resp.raise_for_status()
        resp.close()

    async def renew_lock(
        self, location_url: str, timeout: ty.Optional[aiohttp.ClientTimeout] = None
    ) -> float:
        """Renew the lock on a peek-locked message using the Location URL.

        Returns the updated lock duration in seconds.
        Uses a single attempt (no tenacity retries) since the caller
        (_renew_loop) already retries on its own schedule.
        """
        resp = await self._request("POST", location_url, timeout=timeout, max_retries=1)
        try:
            resp.raise_for_status()
            broker_props = json.loads(resp.headers.get("BrokerProperties", "{}"))
            return _parse_lock_duration(broker_props)
        finally:
            resp.close()

    async def peek_messages(self, n: int = 1) -> ty.List[dict]:
        """Non-destructive peek at messages (no lock acquired)."""
        messages: ty.List[dict] = []

        for _ in range(n):
            url = f"{self._base_url}/{self.queue_name}/messages/head"
            params = {"peekonly": "true"}
            resp = await self._request("POST", url, params=params)
            try:
                if resp.status == 204:
                    break
                resp.raise_for_status()
                body = await resp.text()
                broker_props = json.loads(resp.headers.get("BrokerProperties", "{}"))
                messages.append({"body": body, "broker_properties": broker_props})
            finally:
                resp.close()

        return messages


# ── Envelope ─────────────────────────────────────────────────────────────


class RESTServiceBusEnvelope(Envelope):
    def __init__(
        self,
        message: _ReceivedMessage,
        task: Task,
        client: ServiceBusRestClient,
        lock_renewal_task: ty.Optional[asyncio.Task[None]] = None,
        lock_stop_event: ty.Optional[asyncio.Event] = None,
        lock_lost_event: ty.Optional[asyncio.Event] = None,
    ):
        self.message = message
        self._task = task
        self._client = client
        self._lock_renewal_task = lock_renewal_task
        self._lock_stop_event = lock_stop_event
        self._lock_lost_event = lock_lost_event or asyncio.Event()
        self.done = False

    @property
    def id(self) -> str:
        return self.message.message_id

    @property
    def task(self) -> Task:
        return self._task

    @property
    def lock_lost_event(self) -> asyncio.Event:
        return self._lock_lost_event

    async def cancel_heartbeat(self) -> None:
        if self._lock_stop_event is not None:
            self._lock_stop_event.set()
        if self._lock_renewal_task is not None and not self._lock_renewal_task.done():
            with suppress(asyncio.CancelledError):
                await self._lock_renewal_task
            self._lock_renewal_task = None

    async def requeue(self) -> None:
        LOG.debug(f"Requeueing message {self.id}")
        await self._client.unlock_message(self.message.location_url)

    async def reply(self, response: Response) -> None:
        raise NotImplementedError("REST ServiceBus backend does not support replies yet.")

    async def delete(self, success: bool, error: str | None = None) -> None:
        if success:
            await self._client.complete_message(self.message.location_url)
        else:
            await self._client.deadletter_message(
                self.message.location_url,
                reason=error or "Failed",
            )
        self.done = True
        if self._lock_stop_event is not None:
            self._lock_stop_event.set()

    async def abandon(self) -> None:
        await self._client.unlock_message(self.message.location_url)
        self.done = True
        if self._lock_stop_event is not None:
            self._lock_stop_event.set()

    async def replace(self, task: Task) -> None:
        await self._client.send_message(task.serialize())


# ── Backend ──────────────────────────────────────────────────────────────


class ServiceBusRestBackend(JobQBackend):
    """Service Bus backend that uses pure REST/HTTP calls via aiohttp."""

    def __init__(
        self,
        queue_name: str,
        *,
        fqns: ty.Optional[str] = None,
        credential: ty.Optional[ty.Any] = None,
        exist_ok: bool = True,
        duplicate_detection_window: ty.Optional[timedelta] = None,
    ):
        self.fqns = fqns
        self.queue_name = queue_name
        self.reply_queue_name = queue_name + "-replies"
        self.credential = credential
        self._exist_ok = exist_ok
        self._duplicate_detection_window = duplicate_detection_window or timedelta(days=7)
        self._rest_client: ty.Optional[ServiceBusRestClient] = None
        self._max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 5))
        # max lock renewal lifetime: 3 weeks
        self._max_lock_renewal_seconds = 60 * 60 * 24 * 21

    async def __aenter__(self) -> "ServiceBusRestBackend":
        if self.credential is None:
            raise RuntimeError("No credential provided.")
        assert self.fqns is not None

        self._rest_client = ServiceBusRestClient(
            fqns=self.fqns,
            queue_name=self.queue_name,
            credential=self.credential,
        )
        await self._rest_client.__aenter__()
        await self.create(exist_ok=self._exist_ok)
        await self._warn_if_dedup_misconfigured()
        return self

    async def __aexit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None:
        if self._rest_client is not None:
            await self._rest_client.__aexit__(exc_type, exc, tb)
            self._rest_client = None

    async def _warn_if_dedup_misconfigured(self) -> None:
        from ai4s.jobq.entities import JOBQ_DETERMINISTIC_IDS

        try:
            async with self._get_admin_client() as admin_client:
                props = await admin_client.get_queue(self.queue_name)
                has_dedup = props.requires_duplicate_detection
        except Exception:
            return  # best-effort; don't block startup
        if JOBQ_DETERMINISTIC_IDS and not has_dedup:
            LOG.warning(
                "Deterministic task IDs are enabled but queue %r does not have "
                "duplicate detection enabled. Messages will NOT be deduplicated. "
                "Delete and recreate the queue to fix this.",
                self.queue_name,
            )
        elif not JOBQ_DETERMINISTIC_IDS and has_dedup:
            LOG.warning(
                "Queue %r has duplicate detection enabled but deterministic task IDs "
                "are disabled (JOBQ_DETERMINISTIC_IDS is not set). Random IDs will be "
                "used, so duplicate detection will have no effect.",
                self.queue_name,
            )
        if has_dedup:
            queue_window = props.duplicate_detection_history_time_window
            if queue_window is not None and queue_window != self._duplicate_detection_window:
                LOG.warning(
                    "Queue %r has duplicate detection window %s but configured "
                    "window is %s. Delete and recreate the queue to apply the "
                    "new window.",
                    self.queue_name,
                    queue_window,
                    self._duplicate_detection_window,
                )

    # ── JobQBackend protocol implementation ──────────────────────────────

    async def push(self, task: Task) -> str:
        assert self._rest_client is not None
        return await self._rest_client.send_message(task.serialize(), message_id=task._id)

    async def get_result(self, session_id: str, timeout: ty.Optional[timedelta] = None) -> Response:
        raise NotImplementedError("REST ServiceBus backend does not support get_result yet.")

    def _start_lock_renewal(
        self, message: _ReceivedMessage, interval: float, lock_lost_event: asyncio.Event
    ) -> tuple[asyncio.Task[None], asyncio.Event]:
        """Start a background task that periodically renews the message lock.

        Returns the task and a stop event that can be set to gracefully stop renewal.
        """
        assert self._rest_client is not None
        rest_client = self._rest_client
        deadline = time.monotonic() + self._max_lock_renewal_seconds
        stop_event = asyncio.Event()

        async def _renew_loop() -> None:
            last_success = time.monotonic()
            lock_duration = message.lock_duration_seconds
            # Per-request timeout for each HTTP call in the renewal.
            # Each individual attempt must complete well within the lock
            # duration so we can detect failure before the lock expires.
            per_request_timeout = aiohttp.ClientTimeout(total=max(lock_duration / 3, 5))
            lock_lost_logged = False
            # Maximum immediate retries on transient failure before falling
            # back to the normal sleep interval.  With short lock durations
            # (e.g. 30 s) a single missed renewal can cause lock loss, so we
            # retry quickly a few times before giving up for this cycle.
            max_fast_retries = 2
            try:
                while time.monotonic() < deadline and not stop_event.is_set():
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    if stop_event.is_set():
                        return

                    for retry_attempt in range(1 + max_fast_retries):
                        try:
                            new_duration = await rest_client.renew_lock(
                                message.location_url,
                                timeout=per_request_timeout,
                            )
                            message.lock_duration_seconds = new_duration
                            lock_duration = new_duration
                            per_request_timeout = aiohttp.ClientTimeout(
                                total=max(lock_duration / 3, 5)
                            )
                            last_success = time.monotonic()
                            lock_lost_logged = False
                            LOG.debug("Renewed lock for message %s", message.message_id)
                            break  # success — exit retry loop
                        except asyncio.CancelledError:
                            raise
                        except aiohttp.ClientResponseError as exc:
                            if exc.status == 404:
                                if stop_event.is_set():
                                    LOG.debug(
                                        "Lock renewal 404 for message %s after settlement.",
                                        message.message_id,
                                    )
                                else:
                                    since_last_renewal = time.monotonic() - last_success
                                    LOG.warning(
                                        "Lock lost for message %s: renewal returned 404 "
                                        "(message already settled or lock expired). "
                                        "Last successful renewal was %.0fs ago "
                                        "(lock duration %.0fs). "
                                        "Another worker may process this message.",
                                        message.message_id,
                                        since_last_renewal,
                                        lock_duration,
                                    )
                                    lock_lost_event.set()
                                return
                            if stop_event.is_set():
                                return
                            if retry_attempt < max_fast_retries:
                                LOG.debug(
                                    "Transient renewal failure for message %s "
                                    "(attempt %d/%d): %s — retrying immediately",
                                    message.message_id,
                                    retry_attempt + 1,
                                    1 + max_fast_retries,
                                    exc,
                                )
                                await asyncio.sleep(min(2**retry_attempt, 5))
                                continue
                            LOG.warning(
                                "Failed to renew lock for message %s: %s",
                                message.message_id,
                                exc,
                            )
                        except Exception:
                            if stop_event.is_set():
                                return
                            if retry_attempt < max_fast_retries:
                                LOG.debug(
                                    "Transient renewal failure for message %s "
                                    "(attempt %d/%d) — retrying immediately",
                                    message.message_id,
                                    retry_attempt + 1,
                                    1 + max_fast_retries,
                                )
                                await asyncio.sleep(min(2**retry_attempt, 5))
                                continue
                            LOG.warning(
                                "Failed to renew lock for message %s",
                                message.message_id,
                            )

                    # If we haven't successfully renewed within the lock duration,
                    # the lock has almost certainly expired and another worker may
                    # pick up the message — warn loudly.
                    elapsed = time.monotonic() - last_success
                    if elapsed > lock_duration and not lock_lost_logged:
                        expired_ago = elapsed - lock_duration
                        LOG.warning(
                            "Lock likely expired for message %s: no successful renewal "
                            "for %.0fs (lock duration %.0fs, expired ~%.0fs ago). "
                            "Another worker may process this message.",
                            message.message_id,
                            elapsed,
                            lock_duration,
                            expired_ago,
                        )
                        lock_lost_logged = True
                        lock_lost_event.set()
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
        assert self._rest_client is not None

        # Retry several times before declaring the queue empty.
        # Each REST call is independent and may miss messages that are
        # temporarily locked by other receivers.
        max_empty_polls = int(os.environ.get("JOBQ_SERVICEBUS_EMPTY_POLLS", 4))
        for attempt in range(max_empty_polls):
            try:
                message = await self._rest_client.peek_lock_message(timeout=self._max_wait_time)
                break
            except EmptyQueue:
                if attempt == max_empty_polls - 1:
                    raise
                LOG.debug("No messages on attempt %d/%d, retrying…", attempt + 1, max_empty_polls)
                await asyncio.sleep(attempt)
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt == max_empty_polls - 1:
                    raise EmptyQueue(f"The queue {self.name} is unreachable: {exc}")
                LOG.debug("Transient error on attempt %d/%d: %s", attempt + 1, max_empty_polls, exc)
                await asyncio.sleep(1)

        if message.delivery_count > 1:
            LOG.info(
                "Message %s has been delivered %d times "
                "(previous lock likely expired before processing finished).",
                message.message_id,
                message.delivery_count,
            )

        lock_task: ty.Optional[asyncio.Task[None]] = None
        lock_stop_event: ty.Optional[asyncio.Event] = None
        lock_lost_event = asyncio.Event()
        if with_heartbeat:
            # Renew at half the actual lock duration reported by Service Bus,
            # NOT the application-level visibility_timeout which can be hours.
            interval = max(message.lock_duration_seconds / 2, 5)
            lock_task, lock_stop_event = self._start_lock_renewal(
                message, interval, lock_lost_event
            )

        try:
            task = Task.deserialize(message.body)
        except Exception:
            LOG.error(
                "Stopping processing due to deserialization error to prevent potential data loss.",
                exc_info=True,
            )
            if lock_stop_event is not None:
                lock_stop_event.set()
            if lock_task is not None and not lock_task.done():
                with suppress(asyncio.CancelledError):
                    await lock_task
            raise
        else:
            envelope = RESTServiceBusEnvelope(
                message, task, self._rest_client, lock_task, lock_stop_event, lock_lost_event
            )
            try:
                yield envelope
            finally:
                if lock_stop_event is not None:
                    lock_stop_event.set()
                if lock_task is not None and not lock_task.done():
                    with suppress(asyncio.CancelledError):
                        await lock_task

    async def create(self, exist_ok: bool = True) -> None:
        async with self._get_admin_client() as admin_client:
            try:
                await admin_client.create_queue(
                    queue_name=self.queue_name,
                    requires_session=False,
                    lock_duration=timedelta(minutes=5),
                    requires_duplicate_detection=True,
                    duplicate_detection_history_time_window=self._duplicate_detection_window,
                )
                LOG.info(f"Created queue {self.queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise
            try:
                await admin_client.create_queue(
                    queue_name=self.reply_queue_name,
                    requires_session=True,
                    lock_duration=timedelta(minutes=5),
                )
                LOG.info(f"Created queue {self.reply_queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise

    async def clear(self) -> None:
        """Drain the queue via destructive REST reads."""
        assert self._rest_client is not None
        n = 0
        max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 2))
        while True:
            body = await self._rest_client.receive_and_delete_message(timeout=max_wait_time)
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
        @retry(
            retry=retry_if_result(lambda r: r is None),
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=0.5, max=5, jitter=1),
            reraise=True,
        )
        async def _get_message_count() -> int:
            async with self._get_admin_client() as aclt:
                queue_runtime_info = await aclt.get_queue_runtime_properties(
                    queue_name=self.queue_name
                )
                if queue_runtime_info is None:
                    raise AttributeError("get_queue_runtime_properties returned None")
                ret = queue_runtime_info.active_message_count
                assert isinstance(ret, int)
                return ret

        return await _get_message_count()

    @property
    def name(self) -> str:
        return f"{self.fqns}/{self.queue_name}"

    def generate_sas(self, ttl: timedelta) -> str:
        raise NotImplementedError("REST ServiceBus does not yet support SAS tokens.")

    async def peek(self, n: int = 1, as_json: bool = False) -> ty.List[ty.Any]:
        assert self._rest_client is not None
        messages = await self._rest_client.peek_messages(n)
        if not messages:
            raise EmptyQueue(f"The queue {self.name} has no more tasks.")
        if as_json:
            return [json.loads(m["body"]) for m in messages]
        return messages


# Backward-compatible alias
RESTServiceBusClient = ServiceBusRestBackend
