# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import json
import logging
import os
import signal
import typing as ty
from contextlib import AsyncExitStack, asynccontextmanager

from appdirs import user_cache_dir
from azure.servicebus.aio import ServiceBusClient

from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.pid_file import PidFile, send_signal_by_glob

LOG = logging.getLogger(__name__)


@asynccontextmanager
async def workforce_monitor(worker_id: str, queue_name: str) -> ty.AsyncGenerator[None, None]:
    """Sets up monitoring for service bus events and creates a worker pid file"""

    base_path_for_pid_files = os.environ.get(
        "JOBQ_PID_DIR", user_cache_dir(appname=f"jobq/{queue_name}")
    )

    async def _check_for_shutdown_events() -> None:
        WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE = os.getenv(
            "WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE"
        )
        WORKFORCE_CONTROL_TOPIC_NAME = os.getenv("WORKFORCE_CONTROL_TOPIC_NAME")
        SUBSCRIPTION_NAME = "shutdown"

        if (
            WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE is None
            or WORKFORCE_CONTROL_TOPIC_NAME is None
        ):
            return

        async with AsyncExitStack() as stack:
            credential = await stack.enter_async_context(get_token_credential())

            servicebus_client = await stack.enter_async_context(
                ServiceBusClient(
                    fully_qualified_namespace=WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE,
                    credential=credential,
                ),
            )

            receiver = await stack.enter_async_context(
                servicebus_client.get_subscription_receiver(
                    topic_name=WORKFORCE_CONTROL_TOPIC_NAME,
                    subscription_name=SUBSCRIPTION_NAME,
                ),
            )

            LOG.info(
                "Listening shutdown events from: %s/%s/%s",
                WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE,
                WORKFORCE_CONTROL_TOPIC_NAME,
                SUBSCRIPTION_NAME,
            )

            while True:
                try:
                    received_messages = await receiver.receive_messages(
                        max_message_count=1,
                        max_wait_time=None,
                    )
                    message = received_messages[0]
                    message_dict = json.loads(str(message))
                    if message_dict["operation"] == "do-not-accept-new-tasks":
                        send_signal_by_glob(
                            pattern=f"{base_path_for_pid_files}/*.pid",
                            signal_to_send=signal.SIGUSR1,
                        )
                        await receiver.complete_message(message)
                        break
                    if message_dict["operation"] == "graceful-downscale":
                        send_signal_by_glob(
                            pattern=f"{base_path_for_pid_files}/*.pid",
                            signal_to_send=signal.SIGUSR2,
                        )
                        await receiver.complete_message(message)
                        break
                except asyncio.CancelledError:
                    break
                except Exception:
                    LOG.exception(
                        "Could not json-decode message from topic %r",
                        WORKFORCE_CONTROL_TOPIC_NAME,
                    )

    task = asyncio.create_task(
        _check_for_shutdown_events(),
        name="shutdown_event_monitor",
    )
    try:
        with PidFile(f"{base_path_for_pid_files}/{worker_id}.pid"):
            yield
    finally:
        LOG.info("Stopping workforce monitor")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
