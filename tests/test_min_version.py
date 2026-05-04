# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from importlib.metadata import PackageNotFoundError
from unittest.mock import AsyncMock

from ai4s.jobq.entities import Task


def test_task_min_version_round_trip():
    """min_version survives serialize -> deserialize."""
    task = Task(kwargs={"cmd": "echo hi"}, num_retries=3, min_version="my-package>=2.0.0")
    serialized = task.serialize()
    restored = Task.deserialize(serialized)
    assert restored.min_version == "my-package>=2.0.0"
    assert restored.kwargs == {"cmd": "echo hi"}
    assert restored.num_retries == 3


def test_task_without_min_version_compat():
    """Tasks without min_version round-trip with min_version=None."""
    task = Task(kwargs={"cmd": "echo hi"}, num_retries=1)
    assert task._dict_without_id()["min_version"] is None
    restored = Task.deserialize(task.serialize())
    assert restored.min_version is None


async def test_pull_and_execute_requeues_when_version_too_old(async_queue, mocker):
    """Worker requeues a task when the installed package version is too old."""
    mocker.patch("ai4s.jobq.jobq.asyncio.sleep", new_callable=AsyncMock)
    mocker.patch("ai4s.jobq.jobq.pkg_version", side_effect=lambda name: "1.0.0")

    await async_queue.push({"cmd": "echo hi"}, min_version="my-package>=2.0.0")

    callback = AsyncMock()
    result = await async_queue.pull_and_execute(callback)

    callback.assert_not_awaited()
    assert result is None


async def test_pull_and_execute_requeues_when_package_not_installed(async_queue, mocker):
    """Worker requeues a task when the required package is not installed."""
    mocker.patch("ai4s.jobq.jobq.asyncio.sleep", new_callable=AsyncMock)
    mocker.patch("ai4s.jobq.jobq.pkg_version", side_effect=PackageNotFoundError("my-package"))

    await async_queue.push({"cmd": "echo hi"}, min_version="my-package>=1.0.0")

    callback = AsyncMock()
    result = await async_queue.pull_and_execute(callback)

    callback.assert_not_awaited()
    assert result is None


async def test_pull_and_execute_runs_when_version_sufficient(async_queue, mocker):
    """Worker executes normally when the installed package version meets the requirement."""
    mocker.patch("ai4s.jobq.jobq.pkg_version", side_effect=lambda name: "3.0.0")

    await async_queue.push({"value": 42}, min_version="my-package>=2.0.0")

    calls = []

    async def callback(**kwargs):
        calls.append(kwargs)

    result = await async_queue.pull_and_execute(callback)
    assert result is True
    assert calls == [{"value": 42}]


async def test_pull_and_execute_runs_when_no_min_version(async_queue):
    """Worker executes normally when min_version is not set."""
    await async_queue.push({"value": 1})

    calls = []

    async def callback(**kwargs):
        calls.append(kwargs)

    result = await async_queue.pull_and_execute(callback)
    assert result is True
    assert calls == [{"value": 1}]
