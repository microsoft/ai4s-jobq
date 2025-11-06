# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import errno
import logging
import os
import signal
from glob import glob

LOG = logging.getLogger(__name__)


def send_signal(pid: int, signal_to_send: signal.Signals):
    try:
        os.kill(pid, signal_to_send)
        LOG.info("%s signal sent to process %s.", signal_to_send.name, pid)
    except ProcessLookupError:
        LOG.info("No process with PID %s found.", pid)
    except PermissionError:
        LOG.info("Permission denied to send %s to process %s.", signal_to_send.name, pid)


def send_signal_by_glob(pattern: str, signal_to_send: signal.Signals):
    for pid_file_path in glob(pattern):
        pid = PidFile.check_pid(pid_file=pid_file_path)
        if pid is not None:
            send_signal(pid=pid, signal_to_send=signal_to_send)


class PidFile:
    """
    A context manager that creates pid files in given path, that are acting as a lock mechanism.
    Adapted from https://stackoverflow.com/a/23837022
    """

    def __init__(self, path: str):
        self.pid_file = path

    def __enter__(self):
        try:
            parent_dir = os.path.dirname(self.pid_file)
            os.makedirs(parent_dir, exist_ok=True)
            self.pidfd = os.open(self.pid_file, os.O_CREAT | os.O_WRONLY | os.O_EXCL)
            LOG.info("locked pidfile %s", self.pid_file)
        except OSError as e:
            if e.errno == errno.EEXIST:  # File exists
                pid = self.check_pid(pid_file=self.pid_file)
                if pid is not None:  # This pid file is already in use
                    self.pidfd = None
                    raise ProcessRunningException(
                        "process already running in %s as pid %s", self.pid_file, pid
                    ) from e
                else:
                    os.remove(self.pid_file)
                    LOG.warning("removed stale lockfile %s", self.pid_file)
                    self.pidfd = os.open(self.pid_file, os.O_CREAT | os.O_WRONLY | os.O_EXCL)
            else:
                raise

        os.write(self.pidfd, str(os.getpid()).encode())
        os.close(self.pidfd)
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback) -> bool:
        if exception_type is None:
            self._remove()
            return True
        elif issubclass(exception_type, ProcessRunningException):
            return False
        else:
            if self.pidfd:
                self._remove()
            return False

    def _remove(self):
        LOG.info("removed pidfile %s", self.pid_file)
        os.remove(self.pid_file)

    @staticmethod
    def check_pid(pid_file: str) -> int | None:
        with open(pid_file, "r") as f:
            try:
                pid_str = f.read()
                pid = int(pid_str)
            except ValueError:
                LOG.info("not an integer: %s", pid_str)
                return None
            try:
                # Killing a process with signal `0` does nothing to a running process.
                # It raises an OSError exception with errno=ESRCH if the process does not exist.
                # https://stackoverflow.com/a/13402639
                os.kill(pid, 0)
            except OSError:
                LOG.info("can't deliver signal to %s", pid)
                return None
            else:
                return pid


class ProcessRunningException(BaseException):
    """
    There is an attempt to create a pid file;
    however this pid file is already in use by another process.
    """

    pass
