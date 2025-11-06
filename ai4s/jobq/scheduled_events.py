# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""
Events like VM preemptions are announced by Azure through the scheduled events API.
This module provides a mechanism to poll the scheduled events API and yield the events as they arrive.

https://learn.microsoft.com/en-us/azure/virtual-machines/windows/scheduled-events

Example usage:

```python
import ai4s.jobq.scheduled_events

shutdown_event = asyncio.Event()
async with PreemptionEventHandler(event):
    # wait until preempted
    await shutdown_event.wait()
```
"""

import asyncio
import logging
import os
from dataclasses import dataclass

import aiohttp

from ai4s.jobq import JobQ

LOG = logging.getLogger(__name__)


@dataclass
class ScheduledEvent:
    EventId: str
    EventStatus: str
    EventType: str
    ResourceType: str
    Resources: list[str]
    NotBefore: str
    Description: str
    EventSource: str
    DurationInSeconds: int


class ScheduledEventHandler:
    def __init__(self, callback, poll_interval_seconds: int = 1):
        self._polling_task = None
        self._poll_interval_seconds = poll_interval_seconds
        self.__callback = callback

    async def __aenter__(self):
        if not running_on_azure():
            return self

        self._polling_task = asyncio.create_task(self._poll_scheduled_events())
        self._session = aiohttp.ClientSession()
        await self._session.__aenter__()

        LOG.info("Started polling for scheduled events.")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._polling_task:
            await self._session.__aexit__(exc_type, exc_value, traceback)
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            LOG.info("Stopped polling for scheduled events.")

    async def _poll_scheduled_events(self):
        """Continuously polls the scheduled events API and logs events."""
        try:
            while True:
                events = await self._get_scheduled_events()
                await self.__callback(events)
                await asyncio.sleep(self._poll_interval_seconds)
        except asyncio.CancelledError:
            LOG.info("Polling for scheduled events has been cancelled.")

    async def _get_scheduled_events(self):
        """Fetches scheduled events from the Azure metadata service."""
        # Call 169.254.169.254, a special IP address that Azure VMs can use to access the metadata service.
        # This IP address is local to the VM and is not routable.
        # Docs: https://learn.microsoft.com/en-us/azure/virtual-machines/windows/scheduled-events
        try:
            async with self._session.get(
                "http://169.254.169.254/metadata/scheduledevents",
                headers={"Metadata": "true"},
                params={"api-version": "2020-07-01"},
            ) as resp:
                data = await resp.json()
        except Exception as e:
            LOG.warning(f"Failed to fetch scheduled events: {e}")
            return []
        return [ScheduledEvent(**event) for event in data["Events"]]


class PreemptionEventHandler(ScheduledEventHandler):
    """A simple handler to manage shutdown events."""

    def __init__(
        self,
        shutdown_event: asyncio.Event,
        worker_id: str | None = None,
        queue: JobQ | None = None,
        environment_name: str | None = None,
        *args,
        **kwargs,
    ):
        self.shutdown_event = shutdown_event
        self.worker_id = worker_id
        self.queue = queue
        self.environment_name = environment_name

        super().__init__(self.__callback, *args, **kwargs)

        self._scheduled_event_initiated_by_us = False

        truish = "1 true yes t y".split()
        self.disabled = os.environ.get("JOBQ_DISABLE_SCHEDULED_EVENTS", "0").lower() in truish

    async def __callback(self, events: list[ScheduledEvent]):
        is_preempted = False
        for event in events:
            if event.EventType == "Preempt":
                LOG.warning(
                    f"Preemption event detected! Event ID: {event.EventId!r}, "
                    f"Not Before: {event.NotBefore!r}, Description: {event.Description!r}",
                    extra={
                        "worker_id": self.worker_id,
                        "event": "preemption_detected",
                    },
                )
                is_preempted = True

        if is_preempted:
            if not self.disabled:
                self.shutdown_event.set()
                self._scheduled_event_initiated_by_us = True
        elif self.shutdown_event.is_set() and self._scheduled_event_initiated_by_us:
            LOG.warning(
                "Preemption event is not present anymore, apparently it's obsolete.",
                extra={
                    "worker_id": self.worker_id,
                    "event": "preemption_cleared",
                },
            )
            if not self.disabled:
                self.shutdown_event.clear()
                self._scheduled_event_initiated_by_us = False


def running_on_azure() -> bool:
    # To check if we are on Azure, we check if an environment variable from
    # Azure Batch (backend of AzureML) is set.
    return "AZ_BATCHAI_CLUSTER_NAME" in os.environ
