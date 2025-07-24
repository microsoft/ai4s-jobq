import asyncio
import json
from dataclasses import asdict

import pytest

from ai4s.jobq.scheduled_events import PreemptionEventHandler, ScheduledEvent


class MockResponse:
    def __init__(self, value, status):
        self._value = value
        self.status = status

    async def text(self):
        return json.dumps(self._value)

    async def json(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


class MockCrashResponse:
    def __init__(self, value, status):
        self._value = value
        self.status = status

    async def text(self):
        return json.dumps(self._value)

    async def json(self):
        raise RuntimeError("Connection error")

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


@pytest.mark.asyncio
async def test_aml_preemption_signal_endpoint_canceled(mocker):
    # pretend we're running on AML
    mocker.patch("ai4s.jobq.scheduled_events.running_on_azure", return_value=True)

    # mock the aiohttp.ClientSession.get method
    mocker.patch(
        "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
        return_value=MockResponse(dict(Events=[]), 200),
    )

    shutdown_event = asyncio.Event()

    poll_interval_seconds = 1

    async with PreemptionEventHandler(shutdown_event, poll_interval_seconds) as handler:
        # Ensure the session is created
        assert handler._session is not None

        # Check if the polling task is running
        assert handler._polling_task is not None
        assert not handler._polling_task.done()

        await asyncio.sleep(3)

        assert not shutdown_event.is_set()

        # Simulate a shutdown event

        aml_event = ScheduledEvent(
            EventId="test-event-id",
            EventStatus="Active",
            EventType="Preempt",
            ResourceType="VirtualMachine",
            Resources=["vm1", "vm2"],
            NotBefore="2023-10-01T00:00:00Z",
            Description="Test scheduled event",
            EventSource="Microsoft.Compute",
            DurationInSeconds=3600,
        )

        mocker.patch(
            "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
            side_effect=(
                MockResponse(dict(Events=[asdict(aml_event)]), 200),
                MockResponse(dict(Events=[]), 200),
                MockResponse(dict(Events=[]), 200),
                MockResponse(dict(Events=[]), 200),
            ),
        )

        await asyncio.sleep(3)

        assert not shutdown_event.is_set()
    assert handler._polling_task.done()
    assert handler._session.closed


@pytest.mark.asyncio
async def test_aml_preemption_signal_endpoint(mocker):
    # pretend we're running on AML
    mocker.patch("ai4s.jobq.scheduled_events.running_on_azure", return_value=True)

    # mock the aiohttp.ClientSession.get method
    mocker.patch(
        "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
        return_value=MockResponse(dict(Events=[]), 200),
    )

    shutdown_event = asyncio.Event()

    poll_interval_seconds = 1

    async with PreemptionEventHandler(shutdown_event, poll_interval_seconds) as handler:
        # Ensure the session is created
        assert handler._session is not None

        # Check if the polling task is running
        assert handler._polling_task is not None
        assert not handler._polling_task.done()

        await asyncio.sleep(3)

        assert not shutdown_event.is_set()

        # Simulate a shutdown event

        aml_event = ScheduledEvent(
            EventId="test-event-id",
            EventStatus="Active",
            EventType="Preempt",
            ResourceType="VirtualMachine",
            Resources=["vm1", "vm2"],
            NotBefore="2023-10-01T00:00:00Z",
            Description="Test scheduled event",
            EventSource="Microsoft.Compute",
            DurationInSeconds=3600,
        )

        mocker.patch(
            "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
            return_value=MockResponse(dict(Events=[asdict(aml_event)]), 200),
        )

        await asyncio.sleep(3)

        assert shutdown_event.is_set()
    assert handler._polling_task.done()
    assert handler._session.closed


@pytest.mark.asyncio
async def test_aml_preemption_signal_endpoint_crash(mocker, caplog):
    # pretend we're running on AML
    mocker.patch("ai4s.jobq.scheduled_events.running_on_azure", return_value=True)

    # mock the aiohttp.ClientSession.get method
    mocker.patch(
        "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
        return_value=MockResponse(dict(Events=[]), 200),
    )

    shutdown_event = asyncio.Event()

    poll_interval_seconds = 1

    async with PreemptionEventHandler(shutdown_event, poll_interval_seconds) as handler:
        mocker.patch(
            "ai4s.jobq.scheduled_events.aiohttp.ClientSession.get",
            return_value=MockCrashResponse(dict(Events=[]), 500),
        )

        await asyncio.sleep(3)

    assert not shutdown_event.is_set()
    assert handler._polling_task.done()
    assert handler._session.closed

    assert "Connection error" in caplog.text
