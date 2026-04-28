# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import os
import queue
import shlex
import shutil
import signal
import typing as ty
from abc import ABC, abstractmethod
from asyncio.subprocess import PIPE
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, AsyncExitStack, ExitStack
from functools import partial
from multiprocessing import Manager
from queue import Empty
from tempfile import TemporaryDirectory

from rich.progress import TextColumn

from ai4s.jobq.entities import WorkerCanceled
from ai4s.jobq.ext.background_dirsync import BackgroundDirSync

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self

LOG = logging.getLogger(__name__)
TASK_LOG = logging.getLogger("task")
ResultType = ty.TypeVar("ResultType")
Task = ty.TypeVar("Task")
Seed = ty.TypeVar("Seed")
T = ty.TypeVar("T")


class ProcessPoolRegistry:
    """Registry for process pools."""

    _pools: set["ProcessPool"] = set()

    @classmethod
    def remove_pool(cls, pool: "ProcessPool") -> None:
        cls._pools.remove(pool)

    @classmethod
    def register_pool(cls, pool: "ProcessPool") -> None:
        cls._pools.add(pool)

    @classmethod
    async def wait_for_msg_queue_to_drain(cls) -> None:
        LOG.info("Waiting for all process pool message queues to drain...")
        results = await asyncio.gather(
            *(pool._wait_for_msg_queue_to_drain() for pool in cls._pools),
            return_exceptions=True,
        )
        for pool, result in zip(cls._pools, results):
            if isinstance(result, Exception):
                LOG.error(
                    "Exception while draining message queue for pool %r: %r",
                    pool,
                    result,
                )


if ty.TYPE_CHECKING:

    class _AbstractAsyncContextManager(AbstractAsyncContextManager[T]):
        pass

else:

    class _AbstractAsyncContextManager(ty.Generic[T], AbstractAsyncContextManager):
        pass


def _env_wrapper(
    func: ty.Callable[[], ResultType],
    env: ty.Optional[ty.Dict[str, str]] = None,
) -> ResultType:
    if env:
        for k, v in env.items():
            if not isinstance(v, str):
                env[k] = str(v)
                LOG.warning(
                    f"Value {v!r} for environment variable {k!r} is not a string. Converting to str."
                )
        os.environ.update(env)
    return func()


async def read_stream_and_log(
    stream: asyncio.StreamReader, log_msg_queue: "queue.Queue", job_id: str, level: int
) -> None:
    """puts all lines from stream into queue, together with the level."""
    while True:
        line = await stream.readline()
        if not line:
            # not even '\n'!
            break
        log_msg_queue.put((level, job_id, line.decode("utf-8").rstrip()))


async def run_cmd_and_log_outputs(
    cmd: str,
    log_msg_queue: "queue.Queue",
    job_id: str,
    cwd: ty.Optional[str],
    emulate_tty: bool = False,
) -> int:
    """
    Runs the command in cmd and puts stdout/stderr into a queue.
    """
    # start process
    executable = shutil.which("bash")
    script_executable = shutil.which("script")
    if cwd is not None:
        cmd = f"cd {cwd} ; {cmd}"

    def log(level, msg, *params):
        if params:
            msg = msg % params
        try:
            log_msg_queue.put((level, job_id, msg))
        except Exception:
            print(f"Could not log {level} message: {msg}")

    if executable is not None:
        if not emulate_tty:
            if os.environ.get("JOBQ_USE_LOGIN_SHELL", "0").lower() in (
                "1",
                "true",
                "yes",
            ):
                # use a login shell, so that the user's .bashrc is sourced
                # and conda activate works out of the box.
                # This is not the default anymore and only kept for backwards compatibility.
                # Using login shells messes with Singularity's environment, where
                # login shells `cd` into a specific directory and print a banner.
                args = ["--login", "-c", cmd]
            else:
                args = ["-c", cmd]
        else:
            # use `script` to emulate a TTY, so that the command and subcommands use line buffering
            # and print their output immediately.
            if script_executable is None:
                raise RuntimeError(
                    "Could not find `script` command to emulate TTY. "
                    "Please install it or set `emulate_tty=False`."
                )
            executable = script_executable
            args = ["-q", "-c", cmd, "/dev/null"]

        process = await asyncio.create_subprocess_exec(
            executable, *args, stdout=PIPE, stderr=PIPE, cwd=cwd
        )
    else:
        if emulate_tty:
            quoted_cmd = shlex.quote(cmd)
            cmd = f"script -q -c {quoted_cmd} /dev/null"

        process = await asyncio.create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE, cwd=cwd)

    terminated = False

    def handle_shutdown_signal(*args):
        nonlocal terminated

        # log to stdout
        LOG.info(
            "Received termination signal in pool process %d. Terminating shell subprocess %d",
            os.getpid(),
            process.pid,
        )
        # log to centralized log queue
        log(
            logging.INFO,
            "Received termination signal in %d. Terminating shell subprocess %d",
            os.getpid(),
            process.pid,
        )
        terminated = True
        process.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGUSR1, handle_shutdown_signal)

    # read child's stdout/stderr concurrently
    stdout = process.stdout
    stderr = process.stderr
    assert stdout is not None
    assert stderr is not None

    try:
        await asyncio.gather(
            read_stream_and_log(stdout, log_msg_queue, job_id, logging.INFO),
            read_stream_and_log(stderr, log_msg_queue, job_id, logging.WARNING),
            return_exceptions=True,
        )
    except Exception:
        log(logging.DEBUG, "Error reading stdout/stderr of process %d", process.pid)
        process.kill()
        raise
    finally:
        log(
            logging.DEBUG,
            "Waiting for process %d to exit (terminated: %r)",
            process.pid,
            terminated,
        )
        ret = await process.wait()
        if terminated:
            log(logging.DEBUG, "Raising WorkerCanceled since process was terminated")
            raise WorkerCanceled()
        return ret


class DefaultSeed:
    def __str__(self) -> str:
        return "<DefaultSeed>"

    def __repr__(self) -> str:
        return str(self)


class StackManager(_AbstractAsyncContextManager["StackManager"]):
    def __init__(self) -> None:
        super().__init__()
        self.stack = AsyncExitStack()
        self.context_managers: ty.List[AbstractAsyncContextManager[ty.Any]] = []

    def register_context_manager(self, *manager: _AbstractAsyncContextManager[ty.Any]) -> None:
        """Registers a context manager to be entered and exited with this one."""
        for mgr in manager:
            if not isinstance(mgr, AbstractAsyncContextManager):
                raise TypeError(f"{mgr!r} is not an AbstractAsyncContextManager")
            self.context_managers.append(mgr)

    async def __aenter__(self) -> Self:
        await self.stack.__aenter__()
        for cm in self.context_managers:
            await self.stack.enter_async_context(cm)
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        await self.stack.__aexit__(*args)
        return None


class WorkSpecification(StackManager, ty.Generic[Task, Seed], ABC):
    """Protocol for a work specification.

    This generates a list of tasks that are to be added to a queue.

    - The optional method `task_seeds` can be used to parallelize the generation of tasks with the *mandatory* method `list_tasks`.
    - The optional method `already_done` can be used to check if a task is already done and should not be enqueued.
    - The optional method `enqueue_task` can be used to enqueue a task. If this method is not implemented, the task will be enqueued as-is.

    """

    async def task_seeds(self) -> ty.AsyncGenerator[Seed, None]:
        """
        Return an initial set of seeds for workers running `list_tasks`.
        """
        yield DefaultSeed()  # type: ignore

    def list_tasks(self, seed: Seed, force: bool = False) -> ty.AsyncGenerator[Task, None]:
        """
        Yield kwargs for tasks to be potentially enqueued. If you do not care about/
        implement `task_seeds`, `seed` will be a `DefaultSeed` object that you
        can simply ignore.
        """
        raise NotImplementedError(
            "list_tasks must be implemented in a subclass of WorkSpecification. "
        )

    async def already_done(self, task: Task) -> bool:
        """
        Verify whether a task is already done and should not be enqueued.
        This function returns False by default, and is called by `enqueue_task`.
        """
        return False

    async def enqueue_task(
        self, task: Task, force: bool = False
    ) -> ty.Optional[ty.Union[ty.Dict[str, ty.Any], str]]:
        """
        Enqueue a task. By default, the task is enqueued as-is as long as already_done(task) is False or force is True.
        """
        if force or not await self.already_done(task):
            if not isinstance(task, (dict, str)):
                raise ValueError(
                    f"Task {task!r} must be a dict of json-serializable kwargs or a string."
                )
            return task
        return None


class Processor(StackManager, ABC):
    @abstractmethod
    @ty.no_type_check
    async def __call__(self, **kwargs) -> None: ...

    async def resume(self) -> None:
        pass


class ProcessPool(_AbstractAsyncContextManager["ProcessPool"]):
    """Use this mixin to provide a process pool for any CPU-intensive or blocking work."""

    def __init__(self, pool_size: int = 100) -> None:
        self.pool_size = pool_size
        self.mp_manager = Manager()
        self.log_msg_queue: ty.Optional["queue.Queue"] = None
        self.__pool: ty.Optional[ProcessPoolExecutor] = None
        self._shutdown_lock = asyncio.Lock()
        self._in_shutdown = False
        super().__init__()

    async def resume(self) -> None:
        self._in_shutdown = False

    async def _kill_subprocesses(self, loop) -> None:
        LOG.info(
            "Pool handles shutdown signal in %d: signaling to child processes",
            os.getpid(),
        )

        for pid, p in self._pool._processes.items():
            try:
                if p.is_alive() and pid != os.getpid():
                    LOG.info("Sending termination signal to pool process %d", pid)
                    # SIGINT is ignored by subprocesses, since they would otherwise be killed by the shell
                    # on ctrl-c.
                    os.kill(pid, signal.SIGUSR1)
            except Exception:
                LOG.exception("Error passing signal to pool processes")

        LOG.debug("Shutting down pool.")
        self._pool.shutdown(wait=True)
        LOG.debug("Pool shutdown done.")
        self.__pool = await self._create_pool()
        await asyncio.sleep(1)

    @property
    def _pool(self) -> ProcessPoolExecutor:
        if self.__pool is None:
            raise RuntimeError(
                "Pool not initialized. Use as async context manager, "
                "either yourself or via StackManager.register_context_manager()."
            )
        return self.__pool

    @staticmethod
    def _truish(value: ty.Optional[str]) -> bool:
        if value is None:
            return False
        return value.lower() in ("true", "1", "yes", "y")

    async def submit(
        self,
        func: ty.Callable[[], ResultType],
        bg_dirsync_to: ty.Optional[str] = None,
        env: ty.Optional[ty.Dict[str, str]] = None,
    ) -> ResultType:
        """
        Run a function in a separate process. Make sure all parameters and
        return values are pickleable with little overhead, as this would slow
        down the efficiency of this call.
        """
        loop = asyncio.get_running_loop()

        env = env or {}
        ret = None

        with ExitStack() as stack:
            if bg_dirsync_to:
                tmpdir = stack.enter_context(TemporaryDirectory())
                stack.enter_context(
                    BackgroundDirSync(
                        tmpdir,
                        bg_dirsync_to,
                        delete_after_copy=self._truish(env.get("AMLT_DELETE_AFTER_COPY")),
                        freq=int(env.get("AMLT_DIRSYNC_FREQ", 30)),
                        n_threads=int(env.get("AMLT_DIRSYNC_N_THREADS", 5)),
                        include=env.get("AMLT_DIRSYNC_INCLUDE"),
                        exclude=env.get("AMLT_DIRSYNC_EXCLUDE"),
                        remove_if_not_in_source=False,
                    )
                )
                env["AMLT_DIRSYNC_DIR"] = tmpdir

            try:
                ret = await loop.run_in_executor(self._pool, partial(_env_wrapper, func, env=env))
            except asyncio.CancelledError:
                async with self._shutdown_lock:
                    if not self._in_shutdown:
                        self._in_shutdown = True
                        # ideally, we should only SIGTERM the process submitted above, but we don't know its PID:
                        # this is an implementation detail of ProcessPoolExecutor.
                        # instead, the first worker to receive a SIGTERM will
                        # send a SIGTERM to *all* subprocesses.
                        # For the other processes, we set _in_shutdown to True, so that they
                        # raise WorkerCanceled() instead of returning a (potentially incorrect) result.
                        await self._kill_subprocesses(loop)
            if self._in_shutdown:
                raise WorkerCanceled()
        return ty.cast(ResultType, ret)

    async def _create_pool(self) -> ProcessPoolExecutor:
        def init_process():
            # ignore SIGINT in children, because shell sends it to the whole process group
            # and we want the parent to handle it
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        return ProcessPoolExecutor(self.pool_size, initializer=init_process)

    async def _wait_for_msg_queue_to_drain(self) -> None:
        assert self.log_msg_queue is not None, "Log message queue not initialized"
        while not self.log_msg_queue.empty():
            try:
                await asyncio.sleep(0.1)
            except Exception:
                LOG.exception("Error while waiting for log message queue to drain.")
                break

    async def __aenter__(self) -> Self:
        loop = asyncio.get_running_loop()

        self.mp_manager.__enter__()  # type: ignore
        self.log_msg_queue = self.mp_manager.Queue()

        ProcessPoolRegistry.register_pool(self)

        async def log_from_queue() -> None:
            assert self.log_msg_queue is not None, "Log message queue not initialized."
            with ThreadPoolExecutor(max_workers=1) as ex:
                while True:
                    try:
                        item = await loop.run_in_executor(
                            ex, partial(self.log_msg_queue.get, timeout=1), None
                        )
                    except Empty:
                        continue
                    except Exception:
                        LOG.exception("Error in logging message queue.")
                        continue
                    if item is None:
                        LOG.info("Log message queue closed, exiting log_from_queue.")
                        break
                    try:
                        log = TASK_LOG.getChild(str(item[1]))
                        # use the custom filter coming from CustomDimensionsFilter, so that we have eg queue names
                        # filters do not get inherited automatically by child loggers
                        for f in LOG.filters:
                            log.addFilter(f)
                        log.log(item[0], item[2])
                    except Exception:
                        LOG.exception("Error in logging message queue.")
                        continue

        self._apq_task = asyncio.create_task(log_from_queue(), name="log_from_queue")
        self.__pool = await self._create_pool()
        await super().__aenter__()
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        LOG.debug("Started ProcessPool context manager exit.")
        self._pool.shutdown(wait=True)
        LOG.debug("Signaling log msg queue to stop.")
        if self.log_msg_queue is not None:
            # put a sentinel value to stop the log_from_queue task
            self.log_msg_queue.put(None)
        try:
            LOG.debug("Waiting for log queue task to finish.")
            await self._apq_task
        except asyncio.CancelledError:
            pass
        finally:
            LOG.debug("Log queue task finished.")
        ProcessPoolRegistry.remove_pool(self)
        self.log_msg_queue = None
        self.mp_manager.__exit__(*args)  # type: ignore
        LOG.debug("MP manager exited.")
        await super().__aexit__(*args)
        LOG.debug("Pool exited.")
        return None


class SequentialProcessor(Processor):
    def __init__(self, callback):
        super().__init__()
        self.pool = ProcessPool(pool_size=1)
        self._callback = callback
        self.register_context_manager(self.pool)

    async def resume(self) -> None:
        await self.pool.resume()

    async def __call__(self, **kwargs):
        return await self.pool.submit(partial(self._callback, **kwargs))


class ShellCommandProcessor(Processor):
    """
    Runs a shell command for each task, and forwards the command's
    stdout/stderr to the main processe's logger.

    Multiple tasks can run in parallel, if num_workers > 1.
    """

    def __init__(self, num_workers: int = 1, emulate_tty: bool = False) -> None:
        super().__init__()
        self.pool = ProcessPool(pool_size=num_workers)
        self.register_context_manager(self.pool)
        self.stats = ProgressStats()
        self.emulate_tty = emulate_tty

    @classmethod
    def _subprocess_call(
        cls,
        cmd: str,
        apqueue: "queue.Queue",
        job_id: str,
        cwd: ty.Optional[str],
        emulate_tty: bool = False,
    ) -> int:
        """
        It's not strictly necessary to run this in asyncio, since we're
        already in a separate process. However, asyncio allows us to wait on
        both stdout and stderr at the same time, and forward their output to
        the main process's logger.
        """
        return asyncio.run(run_cmd_and_log_outputs(cmd, apqueue, job_id, cwd, emulate_tty))

    async def resume(self) -> None:
        LOG.info("Resuming ShellCommandProcessor.")
        await self.pool.resume()

    async def __call__(
        self,
        cmd: str,
        _job_id: str,
        bg_dirsync_to: ty.Optional[str] = None,
        env: ty.Optional[ty.Dict[str, str]] = None,
        cwd: ty.Optional[str] = None,
    ) -> int:
        assert self.pool.log_msg_queue is not None, "Log message queue not initialized."
        call = partial(
            self._subprocess_call,
            cmd=cmd,
            apqueue=self.pool.log_msg_queue,
            job_id=_job_id,
            cwd=cwd,
            emulate_tty=self.emulate_tty,
        )
        if env is None:
            env = {}
        env.setdefault("PYTHONUNBUFFERED", "1")  # an empty value means false
        env = {**env, "JOBQ_TASK_ID": _job_id}
        ret_code = await self.pool.submit(call, bg_dirsync_to=bg_dirsync_to, env=env)
        if ret_code != 0:
            self.stats.num_failed_jobq_tasks += 1
            raise RuntimeError(f"Command {cmd!r} failed with return code {ret_code}")
        else:
            self.stats.num_succeeded_jobq_tasks += 1
        return ret_code


class EnqueueStats(TextColumn):
    def __init__(self, text_format: str = "") -> None:
        super().__init__(text_format)
        self.n_considered = 0
        self.n_queued = 0
        self._fixed_text = ""

    def __str__(self) -> str:
        return f"Considered: {self.n_considered}, Queued: {self.n_queued}"

    @property
    def text_format(self) -> str:
        return str(self)

    @text_format.setter
    def text_format(self, value: str) -> None:
        self._fixed_text = value


class ProgressStats(TextColumn):
    def __init__(self, text_format: str = "") -> None:
        super().__init__(text_format)
        self.num_succeeded_jobq_tasks = 0
        self.num_failed_jobq_tasks = 0
        self._fixed_text = ""

    def dict(self) -> ty.Dict[str, ty.Any]:
        return {
            "num_succeeded_tasks": self.num_succeeded_jobq_tasks,
            "num_failed_tasks": self.num_failed_jobq_tasks,
        }

    def __str__(self) -> str:
        return f"Succeeded: {self.num_succeeded_jobq_tasks}, Failed: {self.num_failed_jobq_tasks}"

    @property
    def text_format(self) -> str:
        return str(self)

    @text_format.setter
    def text_format(self, value: str) -> None:
        self._fixed_text = value
