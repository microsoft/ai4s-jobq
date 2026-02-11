# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ai4s.jobq.backend.servicebus_rest import (
    RESTServiceBusClient,
    RESTServiceBusEnvelope,
    RESTServiceBusWorker,
    _CachedTokenCredential,
    _ReceivedMessage,
)
from ai4s.jobq.entities import EmptyQueue, Task

# ── Helpers ──────────────────────────────────────────────────────────────


def _make_credential():
    """Create a mock AsyncTokenCredential."""
    cred = AsyncMock()
    cred.get_token = AsyncMock(return_value=MagicMock(token="test-token", expires_on=9999999999))
    cred.__aenter__ = AsyncMock(return_value=cred)
    cred.__aexit__ = AsyncMock(return_value=None)
    return cred


def _make_task(**kwargs) -> Task:
    return Task(kwargs=kwargs or {"cmd": "echo hello"}, num_retries=0)


def _broker_props(message_id="msg-1", lock_token="lock-1", seq=1, delivery_count=1) -> str:
    return json.dumps(
        {
            "MessageId": message_id,
            "LockToken": lock_token,
            "SequenceNumber": seq,
            "DeliveryCount": delivery_count,
        }
    )


def _mock_response(status=201, body="", headers=None):
    resp = AsyncMock()
    resp.status = status
    resp.raise_for_status = MagicMock()
    resp.text = AsyncMock(return_value=body)
    resp.headers = headers or {}
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=None)
    return resp


# ── Token caching ────────────────────────────────────────────────────────


class TestCachedTokenCredential:
    @pytest.mark.asyncio
    async def test_caches_token(self):
        cred = _make_credential()
        cached = _CachedTokenCredential(cred)
        async with cached:
            t1 = await cached.get_token()
            t2 = await cached.get_token()
            assert t1 == t2 == "test-token"
            cred.get_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_refreshes_expired_token(self):
        cred = _make_credential()
        cached = _CachedTokenCredential(cred)
        async with cached:
            await cached.get_token()
            # Simulate expired token
            cached.token = MagicMock(token="old", expires_on=0)
            t = await cached.get_token()
            assert t == "test-token"
            assert cred.get_token.call_count == 2


# ── _ReceivedMessage parsing ─────────────────────────────────────────────


class TestReceivedMessage:
    def test_from_response(self):
        resp = MagicMock()
        resp.headers = {
            "BrokerProperties": _broker_props("m1", "l1", 42, 3),
            "Location": "https://myns.servicebus.windows.net/testq/messages/42/l1",
        }
        msg = _ReceivedMessage.from_response(resp, "body-content")
        assert msg.message_id == "m1"
        assert msg.lock_token == "l1"
        assert msg.sequence_number == 42
        assert msg.delivery_count == 3
        assert msg.body == "body-content"
        assert msg.location_url == "https://myns.servicebus.windows.net/testq/messages/42/l1"
        assert msg.lock_duration_seconds == 30.0  # default when no LockedUntilUtc


# ── RESTServiceBusEnvelope ───────────────────────────────────────────────


class TestRESTServiceBusEnvelope:
    def _make_envelope(self) -> RESTServiceBusEnvelope:
        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        msg = _ReceivedMessage(
            body="body",
            message_id="msg-1",
            lock_token="lock-1",
            sequence_number=1,
            delivery_count=1,
            location_url=loc,
            lock_duration_seconds=30.0,
        )
        client = AsyncMock(spec=RESTServiceBusClient)
        return RESTServiceBusEnvelope(msg, _make_task(), client)

    @pytest.mark.asyncio
    async def test_delete_success(self):
        env = self._make_envelope()
        await env.delete(success=True)
        env._client._complete_message.assert_awaited_once_with(env.message.location_url)
        assert env.done

    @pytest.mark.asyncio
    async def test_delete_failure_deadletters(self):
        env = self._make_envelope()
        await env.delete(success=False, error="boom")
        env._client._deadletter_message.assert_awaited_once_with(
            env.message.location_url, reason="boom"
        )
        assert env.done

    @pytest.mark.asyncio
    async def test_requeue(self):
        env = self._make_envelope()
        await env.requeue()
        env._client._unlock_message.assert_awaited_once_with(env.message.location_url)

    @pytest.mark.asyncio
    async def test_replace(self):
        env = self._make_envelope()
        new_task = _make_task(cmd="echo replaced")
        await env.replace(new_task)
        env._client._send_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reply_not_implemented(self):
        env = self._make_envelope()
        with pytest.raises(NotImplementedError):
            await env.reply(MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_heartbeat_noop_when_no_task(self):
        env = self._make_envelope()
        await env.cancel_heartbeat()  # should not raise

    @pytest.mark.asyncio
    async def test_cancel_heartbeat_cancels_running_task(self):
        env = self._make_envelope()
        stop_event = asyncio.Event()

        async def _slow():
            await stop_event.wait()

        env._lock_renewal_task = asyncio.create_task(_slow())
        env._lock_stop_event = stop_event
        await env.cancel_heartbeat()
        assert env._lock_renewal_task is None


# ── RESTServiceBusClient ─────────────────────────────────────────────────


class TestRESTServiceBusClient:
    def _make_client(self) -> RESTServiceBusClient:
        return RESTServiceBusClient(
            queue_name="testq",
            fqns="myns.servicebus.windows.net",
            credential=_make_credential(),
        )

    @pytest.mark.asyncio
    async def test_send_message(self):
        client = self._make_client()
        mock_resp = _mock_response(201)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        msg_id = await client._send_message('{"test": true}')
        assert isinstance(msg_id, str)
        mock_session.post.assert_called_once()
        call_kwargs = mock_session.post.call_args
        assert "/testq/messages" in call_kwargs.args[0]

    @pytest.mark.asyncio
    async def test_peek_lock_message(self):
        client = self._make_client()
        task = _make_task()
        body = task.serialize()
        mock_resp = _mock_response(201, body, {"BrokerProperties": _broker_props()})
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        msg = await client._peek_lock_message()
        assert msg.message_id == "msg-1"
        assert msg.body == body

    @pytest.mark.asyncio
    async def test_peek_lock_empty_raises(self):
        client = self._make_client()
        mock_resp = _mock_response(204)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        with pytest.raises(EmptyQueue):
            await client._peek_lock_message()

    @pytest.mark.asyncio
    async def test_complete_message(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = AsyncMock()
        mock_session.delete = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client._complete_message(loc)
        call_args = mock_session.delete.call_args
        assert call_args.args[0] == loc

    @pytest.mark.asyncio
    async def test_unlock_message(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = AsyncMock()
        mock_session.put = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client._unlock_message(loc)
        call_args = mock_session.put.call_args
        assert call_args.args[0] == loc

    @pytest.mark.asyncio
    async def test_deadletter_message(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = AsyncMock()
        mock_session.put = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client._deadletter_message(loc, reason="test fail")
        call_args = mock_session.put.call_args
        assert call_args.args[0] == loc

    @pytest.mark.asyncio
    async def test_renew_lock(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        await client._cached_credential.__aenter__()

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client._renew_lock(loc)
        call_args = mock_session.post.call_args
        assert call_args.args[0] == loc

    @pytest.mark.asyncio
    async def test_clear_drains_queue(self):
        client = self._make_client()
        call_count = 0

        async def _fake_receive_and_delete(timeout=2):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return '{"body": "msg"}'
            return None

        client._receive_and_delete_message = _fake_receive_and_delete  # type: ignore
        await client.clear()
        assert call_count == 4  # 3 messages + 1 empty

    @pytest.mark.asyncio
    async def test_peek_returns_messages(self):
        client = self._make_client()
        client._peek_messages = AsyncMock(  # type: ignore
            return_value=[{"body": '{"key": "val"}', "broker_properties": {}}]
        )
        result = await client.peek(n=1, as_json=True)
        assert result == [{"key": "val"}]

    @pytest.mark.asyncio
    async def test_peek_empty_raises(self):
        client = self._make_client()
        client._peek_messages = AsyncMock(return_value=[])  # type: ignore
        with pytest.raises(EmptyQueue):
            await client.peek()

    @pytest.mark.asyncio
    async def test_push_raises_runtime_error(self):
        client = self._make_client()
        with pytest.raises(RuntimeError):
            await client.push(_make_task())

    @pytest.mark.asyncio
    async def test_name_property(self):
        client = self._make_client()
        assert client.name == "myns.servicebus.windows.net/testq"


# ── RESTServiceBusWorker ─────────────────────────────────────────────────


class TestRESTServiceBusWorker:
    @pytest.mark.asyncio
    async def test_push(self):
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._send_message = AsyncMock(return_value="new-msg-id")
        worker = RESTServiceBusWorker(backend, no_receiver=False, no_sender=False)

        msg_id = await worker.push(_make_task())
        assert msg_id == "new-msg-id"

    @pytest.mark.asyncio
    async def test_receive_message(self):
        task = _make_task()
        loc = "https://myns.servicebus.windows.net/testq/messages/1/l1"
        msg = _ReceivedMessage(
            body=task.serialize(),
            message_id="m1",
            lock_token="l1",
            sequence_number=1,
            delivery_count=1,
            location_url=loc,
            lock_duration_seconds=30.0,
        )
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(return_value=msg)

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        async with worker.receive_message(timedelta(seconds=30)) as env:
            assert env.task.kwargs == task.kwargs
            assert env.id == "m1"

    @pytest.mark.asyncio
    async def test_receive_message_with_heartbeat(self):
        task = _make_task()
        loc = "https://myns.servicebus.windows.net/testq/messages/1/l1"
        msg = _ReceivedMessage(
            body=task.serialize(),
            message_id="m1",
            lock_token="l1",
            sequence_number=1,
            delivery_count=1,
            location_url=loc,
            lock_duration_seconds=30.0,
        )
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(return_value=msg)
        backend._renew_lock = AsyncMock()

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        async with worker.receive_message(timedelta(seconds=30), with_heartbeat=True) as env:
            assert env._lock_renewal_task is not None
            assert not env._lock_renewal_task.done()
        # After exiting, the lock renewal task should be cancelled
        assert env._lock_renewal_task.done()

    @pytest.mark.asyncio
    async def test_receive_empty_queue(self, monkeypatch):
        monkeypatch.setenv("JOBQ_SERVICEBUS_EMPTY_POLLS", "3")
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(side_effect=EmptyQueue("empty"))

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        with pytest.raises(EmptyQueue):
            async with worker.receive_message(timedelta(seconds=30)):
                pass
        # Should have retried 3 times before raising
        assert backend._peek_lock_message.call_count == 3

    @pytest.mark.asyncio
    async def test_receive_retries_on_transient_empty(self):
        """Message arrives on second attempt — should succeed without raising."""
        task = _make_task()
        loc = "https://myns.servicebus.windows.net/testq/messages/1/l1"
        msg = _ReceivedMessage(
            body=task.serialize(),
            message_id="m1",
            lock_token="l1",
            sequence_number=1,
            delivery_count=1,
            location_url=loc,
            lock_duration_seconds=30.0,
        )
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(side_effect=[EmptyQueue("empty"), msg])

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        async with worker.receive_message(timedelta(seconds=30)) as env:
            assert env.id == "m1"
        assert backend._peek_lock_message.call_count == 2

    @pytest.mark.asyncio
    async def test_receive_retries_on_timeout(self, monkeypatch):
        """TimeoutError on first attempt, message on second — should succeed."""
        monkeypatch.setenv("JOBQ_SERVICEBUS_EMPTY_POLLS", "3")
        task = _make_task()
        loc = "https://myns.servicebus.windows.net/testq/messages/1/l1"
        msg = _ReceivedMessage(
            body=task.serialize(),
            message_id="m1",
            lock_token="l1",
            sequence_number=1,
            delivery_count=1,
            location_url=loc,
            lock_duration_seconds=30.0,
        )
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(side_effect=[TimeoutError("timed out"), msg])
        backend.name = "test/testq"

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        async with worker.receive_message(timedelta(seconds=30)) as env:
            assert env.id == "m1"
        assert backend._peek_lock_message.call_count == 2

    @pytest.mark.asyncio
    async def test_receive_timeout_exhausted_raises_empty(self, monkeypatch):
        """All attempts timeout — should raise EmptyQueue."""
        monkeypatch.setenv("JOBQ_SERVICEBUS_EMPTY_POLLS", "2")
        backend = AsyncMock(spec=RESTServiceBusClient)
        backend._peek_lock_message = AsyncMock(side_effect=TimeoutError("timed out"))
        backend.name = "test/testq"

        worker = RESTServiceBusWorker(backend)
        from datetime import timedelta

        with pytest.raises(EmptyQueue):
            async with worker.receive_message(timedelta(seconds=30)):
                pass
        assert backend._peek_lock_message.call_count == 2

    @pytest.mark.asyncio
    async def test_push_with_sender_disabled_raises(self):
        backend = AsyncMock(spec=RESTServiceBusClient)
        worker = RESTServiceBusWorker(backend, no_sender=True)
        with pytest.raises(AssertionError):
            await worker.push(_make_task())

    @pytest.mark.asyncio
    async def test_receive_with_receiver_disabled_raises(self):
        backend = AsyncMock(spec=RESTServiceBusClient)
        worker = RESTServiceBusWorker(backend, no_receiver=True)
        from datetime import timedelta

        with pytest.raises(AssertionError):
            async with worker.receive_message(timedelta(seconds=30)):
                pass
