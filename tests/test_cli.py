import asyncio
import os
import re
import shutil
import signal
import subprocess
import sys
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import pytest
import yaml
from asyncclick.testing import CliRunner
from azure.storage.queue.aio import QueueClient

from ai4s.jobq import cli
from ai4s.jobq.orchestration.manager import TooManyFailuresException
from ai4s.jobq.work import Processor

QUEUE_PORT = os.environ.get("QUEUE_PORT", "10001")
CONNSTR = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    f"QueueEndpoint=http://127.0.0.1:{QUEUE_PORT}/devstoreaccount1;"
)
QUEUE = "testq"


async def cli_cmd(
    *args: str,
    queue_spec: Optional[List[str]] = None,
    options: Optional[List[str]] = None,
    exit_code=0,
) -> str:
    options = options or []
    queue_spec = queue_spec or ["--conn-str", CONNSTR, f"foo/{QUEUE}"]
    if True:
        result = await CliRunner().invoke(
            cli.main, (*options, *queue_spec, *args), catch_exceptions=False
        )
    else:
        cli.main(
            (*options, *queue_spec, *args),
            standalone_mode=False,
            catch_exceptions=False,
        )
    assert result.exit_code == exit_code, result.output
    return result.output


@pytest.fixture(autouse=True)
async def clean_queue():
    try:
        async with QueueClient.from_connection_string(CONNSTR, QUEUE) as queue:
            async for msg in queue.receive_messages():
                await queue.delete_message(msg)
    except Exception:
        pass


class DummyProcessor(Processor):
    def __init__(self, num_workers=1) -> None:
        super().__init__()

    async def __call__(self, cmd, **kwargs) -> None:
        if "raise" in cmd:
            raise Exception(cmd)
        print(cmd)


@pytest.fixture
def dummy_processor(mocker) -> None:
    mocker.patch("ai4s.jobq.cli.ShellCommandProcessor", DummyProcessor)


@pytest.mark.asyncio
async def test_help() -> None:
    await CliRunner().invoke(cli.main, ["--help"])


@pytest.mark.asyncio
async def test_push_pull(dummy_processor) -> None:
    """
    push/pull should affect the size of the queue.
    """
    uuid = str(uuid4())

    s0 = int((await cli_cmd("size")).strip())

    output = await cli_cmd("push", "-c", f"echo {uuid}")
    assert output == ""

    s1 = int((await cli_cmd("size")).strip())

    output = await cli_cmd("pull")
    assert f"echo {uuid}" in output

    s2 = int((await cli_cmd("size")).strip())

    # sizes are approximate.
    assert s0 < s1 >= s2


@pytest.mark.asyncio
async def test_worker(dummy_processor) -> None:
    """
    workers should handle all tasks in the queue until it's empty.
    """
    output = await cli_cmd("push", "-c", "echo hello world1")
    assert output == ""
    output = await cli_cmd("push", "-c", "echo hello world2")
    assert output == ""

    output = await cli_cmd("worker")
    assert "echo hello world1" in output
    assert "echo hello world2" in output


@pytest.mark.asyncio
async def test_retry(caplog) -> None:
    """
    failing jobs should be retried the specified number of times.
    """
    output = await cli_cmd("push", "-c", "exit 1", "--num-retries", "2")
    assert output == ""

    proc_spec = ["-vv"]
    with pytest.raises(TooManyFailuresException):
        await cli_cmd("worker", "--visibility-timeout", "2s", options=proc_spec)
    assert str(caplog.messages).count("Re-queue") == 2

    await asyncio.sleep(1)
    caplog.clear()
    await cli_cmd("worker", options=proc_spec)
    assert str(caplog.messages).count("Re-queue") == 0


@pytest.mark.asyncio
async def test_envvars(caplog) -> None:
    """
    environment variables passed on the command line should be available in the worker
    """
    await cli_cmd("push", "-c", "echo XYZ-$VALUE", "-e", "VALUE=42")

    caplog.clear()
    await cli_cmd("pull")
    output = str(caplog.messages)
    assert "XYZ-42" in output


@pytest.mark.asyncio
async def test_dirsync(tmp_path) -> None:
    """
    files written to $AMLT_DIRSYNC_DIR should sync to the specified directory.
    """
    await cli_cmd(
        "push",
        "-c",
        "echo hello > $AMLT_DIRSYNC_DIR/foo.txt",
        "--bg-dirsync-to",
        str(tmp_path),
    )
    await cli_cmd("pull")
    assert tmp_path.joinpath("foo.txt").read_text() == "hello\n"


@pytest.mark.asyncio
async def test_parallel() -> None:
    """
    multiple workers can run in parallel
    """
    import time

    await cli_cmd("push", "-c", "sleep 3")
    await cli_cmd("push", "-c", "sleep 3")
    await cli_cmd("push", "-c", "sleep 3")
    start = time.time()
    await cli_cmd("worker", "-n", "3")
    elapsed = time.time() - start
    assert elapsed < 6


@pytest.mark.asyncio
async def test_amlt(mocker, tmp_path) -> None:
    """
    Check that we can launch worker via amlt
    """
    config_yml = tmp_path / "config.yml"
    config_yml.write_text(
        """
environment:
  image: foo
    """
    )

    config_dct = None
    env = None

    class DummyPopen:
        def __init__(self, cmd, **kwargs):
            nonlocal config_dct, env
            augmented_config = cmd[2]
            env = kwargs.get("env")
            with open(augmented_config, "r") as f:
                config_dct = yaml.safe_load(f)

        def wait(self):
            return 0

    mocker.patch("subprocess.Popen", DummyPopen)
    await cli_cmd("amlt", "--time-limit", "1h", "run", str(config_yml))
    assert config_dct
    assert env
    assert config_dct["environment"]["env"]["JOBQ_STORAGE"] == "foo" == env["JOBQ_STORAGE"]
    assert config_dct["environment"]["env"]["JOBQ_QUEUE"] == "testq" == env["JOBQ_QUEUE"]
    assert config_dct["environment"]["env"].get("JOBQ_CREDENTIAL") is None
    assert config_dct["environment"]["env"]["JOBQ_TIME_LIMIT"] == "3600s" == env["JOBQ_TIME_LIMIT"]


@pytest.mark.asyncio
async def test_signal_handling(mocker, tmp_path) -> None:
    """
    Check that we can cleanly abort workers when the top level process receives a SIGTERM.
    """

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'sleep 10 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!',
    )
    await cli_cmd(
        "push",
        "-c",
        'sleep 10 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!',
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "--conn-str=" + CONNSTR,
        "foo/" + QUEUE,
        "worker",
        "-n",
        "2",
        "--visibility-timeout=30s",
    )
    with subprocess.Popen(
        worker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        with pytest.raises(subprocess.TimeoutExpired):
            proc.communicate(timeout=2)
        start = datetime.now()
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate()
        end = datetime.now()

    assert "bash got signal-A" in (stderr + stdout)
    assert "bash got signal-B" in (stderr + stdout)
    assert (stderr + stdout).count("Requeueing") == 2
    # should be done in less than the task's sleep time
    assert (end - start).total_seconds() < 2

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    async with QueueClient.from_connection_string(CONNSTR, QUEUE) as queue:
        async for msg in queue.receive_messages(timeout=1):
            n += 1
    assert n == 2


@pytest.mark.asyncio
async def test_signal_handling_resume(mocker, tmp_path) -> None:
    """
    Check that workers receives SIGTERM but then pick up next task when not preempted.
    """

    mocker.patch.dict(
        os.environ,
        {"JOBQ_PREEMPTION_TIMEOUT": "2"},
    )

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'sleep 4 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!; exit 0',
    )
    await cli_cmd(
        "push",
        "-c",
        'sleep 3 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!; exit 0',
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "--conn-str=" + CONNSTR,
        "foo/" + QUEUE,
        "worker",
        "--visibility-timeout=30s",
    )
    with subprocess.Popen(
        worker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        with pytest.raises(subprocess.TimeoutExpired):
            proc.communicate(timeout=2)
        start = datetime.now()
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate()
        end = datetime.now()

    assert "No preemption occurred within 2 seconds. Resuming work." in re.sub(
        r"\s+", " ", stdout
    ), stdout + stderr
    assert (stderr + stdout).count("bash got signal-A") == 1
    assert (stderr + stdout).count("bash got signal-B") == 0
    assert (stderr + stdout).count("Requeueing") == 1
    # should be done in less than the task's sleep time
    assert (end - start).total_seconds() < 11

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    async with QueueClient.from_connection_string(CONNSTR, QUEUE) as queue:
        async for msg in queue.receive_messages(timeout=1):
            n += 1
    assert n == 0


# broken in devops CI for unknown reasons?
#     props = await clean_queue.get_queue_properties()
#     # reappears immediately despite longer visibility timeout
#     assert props["approximate_message_count"] == 1


@pytest.mark.asyncio
async def test_cli_proc_spec(mocker, tmp_path) -> None:
    """
    Check that we can launch worker using an arbitrary Processor class via ai4s jobq cli
    """

    tmp_path.joinpath("procpkg").mkdir()
    tmp_path.joinpath("procpkg").joinpath("dummy_proc.py").write_text(
        """
from contextlib import AbstractAsyncContextManager
class DummyProcessor(AbstractAsyncContextManager):
  async def __call__(self, cmd, **kwargs):
    print("Called with cmd:", cmd)

  async def __aenter__(self):
    return self

  async def __aexit__(self, exc_type, exc_value, traceback):
    pass
"""
    )
    new_sys_path = [str(tmp_path)] + list(sys.path)
    mocker.patch.object(sys, "path", new_sys_path)
    push_res = await cli_cmd("push", "-c", "custom hello world", options=["-vv"])
    assert not push_res
    await asyncio.sleep(1)
    pull_res = await cli_cmd("pull", "--proc", "procpkg.dummy_proc.DummyProcessor", options=["-vv"])
    assert "Called with cmd: custom hello world" in pull_res
