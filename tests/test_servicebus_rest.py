# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from ai4s.jobq.backend.servicebus_rest import (
    RESTServiceBusEnvelope,
    ServiceBusRestBackend,
    ServiceBusRestClient,
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
    resp.close = MagicMock()
    resp.request_info = MagicMock()
    resp.history = ()
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
        client = AsyncMock(spec=ServiceBusRestClient)
        return RESTServiceBusEnvelope(msg, _make_task(), client)

    @pytest.mark.asyncio
    async def test_delete_success(self):
        env = self._make_envelope()
        await env.delete(success=True)
        env._client.complete_message.assert_awaited_once_with(env.message.location_url)
        assert env.done

    @pytest.mark.asyncio
    async def test_delete_failure_deadletters(self):
        env = self._make_envelope()
        await env.delete(success=False, error="boom")
        env._client.deadletter_message.assert_awaited_once_with(
            env.message.location_url,
            sequence_number=env.message.sequence_number,
            lock_token=env.message.lock_token,
            reason="boom",
        )
        assert env.done

    @pytest.mark.asyncio
    async def test_requeue(self):
        env = self._make_envelope()
        await env.requeue()
        env._client.unlock_message.assert_awaited_once_with(env.message.location_url)

    @pytest.mark.asyncio
    async def test_replace(self):
        env = self._make_envelope()
        new_task = _make_task(cmd="echo replaced")
        await env.replace(new_task)
        # replace() is a no-op on Service Bus — must NOT send a new message
        env._client.send_message.assert_not_awaited()

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


# ── ServiceBusRestClient ────────────────────────────────────────────────


class TestServiceBusRestClient:
    def _make_client(self) -> ServiceBusRestClient:
        return ServiceBusRestClient(
            fqns="myns.servicebus.windows.net",
            queue_name="testq",
            credential=_make_credential(),
        )

    def _setup_client(self, client: ServiceBusRestClient, mock_resp):
        """Wire up a mock session and credential for a client."""
        mock_session = AsyncMock()
        mock_session.request = AsyncMock(return_value=mock_resp)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        # Inline __aenter__ for the cached credential
        client._cached_credential.token = None
        return mock_session

    @pytest.mark.asyncio
    async def test_send_message(self):
        client = self._make_client()
        mock_resp = _mock_response(201)
        mock_session = self._setup_client(client, mock_resp)

        msg_id = await client.send_message('{"test": true}')
        assert isinstance(msg_id, str)
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        assert call_args.args[0] == "POST"
        assert "/testq/messages" in call_args.args[1]

    @pytest.mark.asyncio
    async def test_send_message_uses_provided_message_id(self):
        """When message_id is provided, it is used as the MessageId in BrokerProperties."""
        client = self._make_client()
        mock_resp = _mock_response(201)
        mock_session = self._setup_client(client, mock_resp)

        msg_id = await client.send_message('{"test": true}', message_id="custom-id-123")
        assert msg_id == "custom-id-123"
        call_headers = mock_session.request.call_args.kwargs.get("headers", {})
        bp = json.loads(call_headers["BrokerProperties"])
        assert bp["MessageId"] == "custom-id-123"

    @pytest.mark.asyncio
    async def test_peek_lock_message(self):
        client = self._make_client()
        task = _make_task()
        body = task.serialize()
        mock_resp = _mock_response(201, body, {"BrokerProperties": _broker_props()})
        self._setup_client(client, mock_resp)

        msg = await client.peek_lock_message()
        assert msg.message_id == "msg-1"
        assert msg.body == body

    @pytest.mark.asyncio
    async def test_peek_lock_empty_raises(self):
        client = self._make_client()
        mock_resp = _mock_response(204)
        self._setup_client(client, mock_resp)

        with pytest.raises(EmptyQueue):
            await client.peek_lock_message()

    @pytest.mark.asyncio
    async def test_complete_message(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = self._setup_client(client, mock_resp)

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client.complete_message(loc)
        call_args = mock_session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert call_args.args[1] == loc

    @pytest.mark.asyncio
    async def test_unlock_message(self):
        client = self._make_client()
        mock_resp = _mock_response(200)
        mock_session = self._setup_client(client, mock_resp)

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        await client.unlock_message(loc)
        call_args = mock_session.request.call_args
        assert call_args.args[0] == "PUT"
        assert call_args.args[1] == loc

    @pytest.mark.asyncio
    async def test_deadletter_message(self):
        """Dead-lettering settles via AMQP management link using the REST lock token."""
        from unittest.mock import patch

        client = self._make_client()

        lock_token = "e836f908-afe0-47e3-b06a-509ba4769bed"

        # Mock the AMQP receiver's mgmt request
        mock_receiver = AsyncMock()
        mock_receiver._mgmt_request_response_with_retry = AsyncMock()
        mock_receiver.__aenter__ = AsyncMock(return_value=mock_receiver)
        mock_receiver.__aexit__ = AsyncMock(return_value=None)

        mock_sb_client = AsyncMock()
        mock_sb_client.get_queue_receiver = MagicMock(return_value=mock_receiver)
        mock_sb_client.__aenter__ = AsyncMock(return_value=mock_sb_client)
        mock_sb_client.__aexit__ = AsyncMock(return_value=None)

        mock_sb_cls = MagicMock(return_value=mock_sb_client)

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        with patch("azure.servicebus.aio.ServiceBusClient", mock_sb_cls):
            await client.deadletter_message(
                loc, sequence_number=42, lock_token=lock_token, reason="test fail"
            )

        # Verify AMQP mgmt link was called with the right disposition
        mock_receiver._mgmt_request_response_with_retry.assert_awaited_once()
        call_args = mock_receiver._mgmt_request_response_with_retry.call_args
        mgmt_message = call_args.args[1]
        assert mgmt_message["disposition-status"] == "suspended"
        assert mgmt_message["deadletter-reason"] == "test fail"

    @pytest.mark.asyncio
    async def test_renew_lock(self):
        client = self._make_client()
        mock_resp = _mock_response(
            200,
            headers={"BrokerProperties": json.dumps({"LockedUntilUtc": ""})},
        )
        mock_session = self._setup_client(client, mock_resp)

        loc = "https://myns.servicebus.windows.net/testq/messages/1/lock-1"
        duration = await client.renew_lock(loc)
        call_args = mock_session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == loc
        assert isinstance(duration, float)


# ── ServiceBusRestBackend ───────────────────────────────────────────────


class TestServiceBusRestBackend:
    def _make_backend(self) -> ServiceBusRestBackend:
        return ServiceBusRestBackend(
            queue_name="testq",
            fqns="myns.servicebus.windows.net",
            credential=_make_credential(),
        )

    @pytest.mark.asyncio
    async def test_push(self):
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.send_message = AsyncMock(return_value="new-msg-id")
        backend._rest_client = rest_client

        msg_id = await backend.push(_make_task())
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
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(return_value=msg)
        backend._rest_client = rest_client

        from datetime import timedelta

        async with backend.receive_message(timedelta(seconds=30)) as env:
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
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(return_value=msg)
        rest_client.renew_lock = AsyncMock(return_value=30.0)
        backend._rest_client = rest_client

        from datetime import timedelta

        async with backend.receive_message(timedelta(seconds=30), with_heartbeat=True) as env:
            assert env._lock_renewal_task is not None
            assert not env._lock_renewal_task.done()
        # After exiting, the lock renewal task should be cancelled
        assert env._lock_renewal_task.done()

    @pytest.mark.asyncio
    async def test_receive_empty_queue(self, monkeypatch):
        monkeypatch.setenv("JOBQ_SERVICEBUS_EMPTY_POLLS", "3")
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(side_effect=EmptyQueue("empty"))
        backend._rest_client = rest_client

        from datetime import timedelta

        with pytest.raises(EmptyQueue):
            async with backend.receive_message(timedelta(seconds=30)):
                pass
        # Should have retried 3 times before raising
        assert rest_client.peek_lock_message.call_count == 3

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
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(side_effect=[EmptyQueue("empty"), msg])
        backend._rest_client = rest_client

        from datetime import timedelta

        async with backend.receive_message(timedelta(seconds=30)) as env:
            assert env.id == "m1"
        assert rest_client.peek_lock_message.call_count == 2

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
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(side_effect=[TimeoutError("timed out"), msg])
        backend._rest_client = rest_client

        from datetime import timedelta

        async with backend.receive_message(timedelta(seconds=30)) as env:
            assert env.id == "m1"
        assert rest_client.peek_lock_message.call_count == 2

    @pytest.mark.asyncio
    async def test_receive_timeout_exhausted_raises_empty(self, monkeypatch):
        """All attempts timeout — should raise EmptyQueue."""
        monkeypatch.setenv("JOBQ_SERVICEBUS_EMPTY_POLLS", "2")
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_lock_message = AsyncMock(side_effect=TimeoutError("timed out"))
        backend._rest_client = rest_client
        backend._rest_client.fqns = "test"
        backend._rest_client.queue_name = "testq"

        from datetime import timedelta

        with pytest.raises(EmptyQueue):
            async with backend.receive_message(timedelta(seconds=30)):
                pass
        assert rest_client.peek_lock_message.call_count == 2

    @pytest.mark.asyncio
    async def test_clear_drains_queue(self):
        backend = self._make_backend()
        call_count = 0

        async def _fake_receive_and_delete(timeout=2):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return '{"body": "msg"}'
            return None

        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.receive_and_delete_message = _fake_receive_and_delete
        backend._rest_client = rest_client
        await backend.clear()
        assert call_count == 4  # 3 messages + 1 empty

    @pytest.mark.asyncio
    async def test_peek_returns_messages(self):
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_messages = AsyncMock(
            return_value=[{"body": '{"key": "val"}', "broker_properties": {}}]
        )
        backend._rest_client = rest_client
        result = await backend.peek(n=1, as_json=True)
        assert result == [{"key": "val"}]

    @pytest.mark.asyncio
    async def test_peek_empty_raises(self):
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.peek_messages = AsyncMock(return_value=[])
        backend._rest_client = rest_client
        with pytest.raises(EmptyQueue):
            await backend.peek()

    @pytest.mark.asyncio
    async def test_push_passes_task_id_as_message_id(self):
        """push() forwards task._id to send_message as message_id."""
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.send_message = AsyncMock(return_value="new-msg-id")
        backend._rest_client = rest_client

        task = _make_task()
        await backend.push(task)
        rest_client.send_message.assert_called_once_with(task.serialize(), message_id=task._id)

    @pytest.mark.asyncio
    async def test_push_deterministic_ids_identical_tasks(self):
        """Two identical tasks produce the same message_id (deterministic IDs are the default)."""
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.send_message = AsyncMock(return_value="msg-id")
        backend._rest_client = rest_client

        t1 = Task(kwargs={"cmd": "echo hi"}, num_retries=0)
        t2 = Task(kwargs={"cmd": "echo hi"}, num_retries=0)
        assert t1._id == t2._id

        await backend.push(t1)
        await backend.push(t2)
        id1 = rest_client.send_message.call_args_list[0].kwargs["message_id"]
        id2 = rest_client.send_message.call_args_list[1].kwargs["message_id"]
        assert id1 == id2

    @pytest.mark.asyncio
    async def test_push_random_ids_when_deterministic_disabled(self, monkeypatch):
        """Two identical tasks get different IDs when JOBQ_DETERMINISTIC_IDS is disabled."""
        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", False)
        backend = self._make_backend()
        rest_client = AsyncMock(spec=ServiceBusRestClient)
        rest_client.send_message = AsyncMock(return_value="msg-id")
        backend._rest_client = rest_client

        t1 = Task(kwargs={"cmd": "echo hi"}, num_retries=0)
        t2 = Task(kwargs={"cmd": "echo hi"}, num_retries=0)
        assert t1._id != t2._id

        await backend.push(t1)
        await backend.push(t2)
        id1 = rest_client.send_message.call_args_list[0].kwargs["message_id"]
        id2 = rest_client.send_message.call_args_list[1].kwargs["message_id"]
        assert id1 != id2

    @pytest.mark.asyncio
    async def test_name_property(self):
        backend = self._make_backend()
        assert backend.name == "myns.servicebus.windows.net/testq"

    @pytest.mark.asyncio
    async def test_warn_dedup_enabled_but_no_queue_dedup(self, monkeypatch, caplog):
        """Warns when deterministic IDs are on but queue lacks duplicate detection."""
        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", True)
        backend = self._make_backend()

        mock_props = MagicMock()
        mock_props.requires_duplicate_detection = False
        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(return_value=mock_props)
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert (
            "duplicate detection enabled" in caplog.text.lower()
            or "will NOT be deduplicated" in caplog.text
        )

    @pytest.mark.asyncio
    async def test_warn_queue_dedup_but_no_deterministic_ids(self, monkeypatch, caplog):
        """Warns when queue has dedup but deterministic IDs are off."""
        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", False)
        backend = self._make_backend()

        mock_props = MagicMock()
        mock_props.requires_duplicate_detection = True
        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(return_value=mock_props)
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert "duplicate detection will have no effect" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_no_warn_when_consistent(self, monkeypatch, caplog):
        """No warning when deterministic IDs and queue dedup are both enabled."""
        from datetime import timedelta

        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", True)
        backend = self._make_backend()

        mock_props = MagicMock()
        mock_props.requires_duplicate_detection = True
        mock_props.duplicate_detection_history_time_window = timedelta(days=7)
        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(return_value=mock_props)
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert caplog.text == ""

    @pytest.mark.asyncio
    async def test_warn_suppressed_on_admin_error(self, monkeypatch, caplog):
        """No warning or crash when admin client throws."""
        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", True)
        backend = self._make_backend()

        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(side_effect=RuntimeError("connection failed"))
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert "deduplicated" not in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_warn_dedup_window_mismatch(self, monkeypatch, caplog):
        """Warns when the queue's dedup window differs from the configured one."""
        from datetime import timedelta

        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", True)
        backend = self._make_backend()
        backend._duplicate_detection_window = timedelta(days=7)

        mock_props = MagicMock()
        mock_props.requires_duplicate_detection = True
        mock_props.duplicate_detection_history_time_window = timedelta(days=1)
        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(return_value=mock_props)
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert "duplicate detection window" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_no_warn_dedup_window_matches(self, monkeypatch, caplog):
        """No warning when queue dedup window matches the configured one."""
        from datetime import timedelta

        monkeypatch.setattr("ai4s.jobq.entities.JOBQ_DETERMINISTIC_IDS", True)
        backend = self._make_backend()
        backend._duplicate_detection_window = timedelta(days=7)

        mock_props = MagicMock()
        mock_props.requires_duplicate_detection = True
        mock_props.duplicate_detection_history_time_window = timedelta(days=7)
        mock_admin = AsyncMock()
        mock_admin.get_queue = AsyncMock(return_value=mock_props)
        mock_admin.__aenter__ = AsyncMock(return_value=mock_admin)
        mock_admin.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(backend, "_get_admin_client", lambda: mock_admin)

        import logging

        with caplog.at_level(logging.WARNING, logger="ai4s.jobq"):
            await backend._warn_if_dedup_misconfigured()
        assert caplog.text == ""


@pytest.mark.asyncio
async def test_lock_renewal_preserves_initial_lock_duration(monkeypatch):
    backend = ServiceBusRestBackend(
        queue_name="testq",
        fqns="myns.servicebus.windows.net",
        credential=_make_credential(),
    )
    backend._rest_client = AsyncMock(spec=ServiceBusRestClient)
    backend._max_lock_renewal_seconds = 1

    msg = _ReceivedMessage(
        body="body",
        message_id="msg-1",
        lock_token="lock-1",
        sequence_number=1,
        delivery_count=1,
        location_url="https://myns.servicebus.windows.net/testq/messages/1/lock-1",
        lock_duration_seconds=300.0,
    )

    backend._rest_client.renew_lock = AsyncMock(return_value=30.0)

    async def fast_wait(awaitable, timeout=None):
        if hasattr(awaitable, "close"):
            awaitable.close()

    monkeypatch.setattr(asyncio, "wait_for", fast_wait)

    lock_lost_event = asyncio.Event()
    task, stop_event = backend._start_lock_renewal(msg, interval=300, lock_lost_event=lock_lost_event)

    await asyncio.sleep(0)
    stop_event.set()
    await task

    backend._rest_client.renew_lock.assert_awaited()
    timeout_arg = backend._rest_client.renew_lock.await_args.kwargs["timeout"]
    assert timeout_arg.total == 100.0
    assert not lock_lost_event.is_set()


class TestServiceBusRestClientRetry:
    def _make_client(self, max_retries: int = 4) -> ServiceBusRestClient:
        client = ServiceBusRestClient(
            fqns="myns.servicebus.windows.net",
            queue_name="testq",
            credential=_make_credential(),
        )
        client._max_retries = max_retries
        return client

    def _setup_client(self, client: ServiceBusRestClient, side_effect):
        mock_session = AsyncMock()
        mock_session.request = AsyncMock(side_effect=side_effect)
        client._session = mock_session
        client._cached_credential = _CachedTokenCredential(_make_credential())
        client._cached_credential.token = None
        return mock_session

    @pytest.mark.asyncio
    async def test_retries_on_connection_error(self):
        """Connection error on first attempt, success on second."""
        client = self._make_client(max_retries=3)
        ok_resp = _mock_response(200)
        self._setup_client(
            client,
            side_effect=[aiohttp.ClientConnectionError("conn reset"), ok_resp],
        )

        resp = await client._request("GET", "https://example.com")
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_retries_on_503(self):
        """503 on first attempt, success on second."""
        client = self._make_client(max_retries=3)
        error_resp = _mock_response(503)
        error_resp.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=503
            )
        )
        ok_resp = _mock_response(200)
        # _request checks status internally — a 503 response gets returned,
        # but the caller (e.g. complete_message) calls raise_for_status.
        # The retry is on the _is_retryable check. Since _request doesn't
        # call raise_for_status itself, we need to test via a public method.
        mock_session = self._setup_client(client, side_effect=[error_resp, ok_resp])

        # complete_message calls raise_for_status on the response
        # But _request returns the response as-is for non-401 codes.
        # So the retry should happen when the *session.request* raises.
        # Let's test with session.request raising directly instead.
        mock_session.request = AsyncMock(
            side_effect=[
                aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=503),
                ok_resp,
            ]
        )
        resp = await client._request("DELETE", "https://example.com/msg/1/lock")
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_retries_on_429(self):
        """429 throttling triggers retry."""
        client = self._make_client(max_retries=3)
        ok_resp = _mock_response(200)
        mock_session = self._setup_client(
            client,
            side_effect=[
                aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=429),
                ok_resp,
            ],
        )

        resp = await client._request("POST", "https://example.com")
        assert resp.status == 200
        assert mock_session.request.call_count == 2

    @pytest.mark.asyncio
    async def test_retries_on_timeout(self):
        """asyncio.TimeoutError triggers retry."""
        client = self._make_client(max_retries=3)
        ok_resp = _mock_response(200)
        self._setup_client(
            client,
            side_effect=[asyncio.TimeoutError(), ok_resp],
        )

        resp = await client._request("GET", "https://example.com")
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_401_triggers_token_refresh_and_retry(self):
        """401 response forces token refresh and retries."""
        client = self._make_client(max_retries=3)
        unauth_resp = _mock_response(401)
        ok_resp = _mock_response(200)
        mock_session = self._setup_client(
            client,
            side_effect=[unauth_resp, ok_resp],
        )

        resp = await client._request("GET", "https://example.com")
        assert resp.status == 200
        # Token should have been invalidated
        assert mock_session.request.call_count == 2

    @pytest.mark.asyncio
    async def test_non_retryable_error_propagates(self):
        """A 409 Conflict should not be retried."""
        client = self._make_client(max_retries=3)
        mock_session = self._setup_client(
            client,
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=409
            ),
        )

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await client._request("PUT", "https://example.com")
        assert exc_info.value.status == 409
        assert mock_session.request.call_count == 1

    @pytest.mark.asyncio
    async def test_exhausted_retries_raises(self):
        """After max retries, the last error propagates."""
        client = self._make_client(max_retries=2)
        self._setup_client(
            client,
            side_effect=aiohttp.ClientConnectionError("conn reset"),
        )

        with pytest.raises(aiohttp.ClientConnectionError):
            await client._request("GET", "https://example.com")

    @pytest.mark.asyncio
    async def test_send_message_uses_same_id_on_retry(self):
        """send_message generates MessageId once, so retries are idempotent."""
        import aiohttp as _aiohttp

        client = self._make_client(max_retries=3)
        ok_resp = _mock_response(201)
        mock_session = self._setup_client(
            client,
            side_effect=[_aiohttp.ClientConnectionError("reset"), ok_resp],
        )

        msg_id = await client.send_message('{"x": 1}')
        assert isinstance(msg_id, str)
        # Both calls should have the same MessageId in BrokerProperties
        call1_headers = mock_session.request.call_args_list[0].kwargs.get("headers", {})
        call2_headers = mock_session.request.call_args_list[1].kwargs.get("headers", {})
        bp1 = json.loads(call1_headers.get("BrokerProperties", "{}"))
        bp2 = json.loads(call2_headers.get("BrokerProperties", "{}"))
        assert bp1["MessageId"] == bp2["MessageId"] == msg_id
