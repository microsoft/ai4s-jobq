# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import atexit
import logging.handlers
import multiprocessing as mp
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ContextDecorator
from fnmatch import fnmatch
from multiprocessing import process, queues, synchronize
from threading import Thread
from typing import Any, Literal, Optional, Sequence, Set

FLUSH_FILENAME = ".amlt_flush_upload"


class DummyException(Exception):
    pass


class Syncer:
    """
    Copies non-existing/newer files from src to dst.
    Blocking.
    """

    def __init__(
        self,
        src: str,
        dst: str,
        logger: logging.Logger,
        n_threads: int = 10,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        delete_after_copy: bool = False,
        remove_if_not_in_source: bool = True,
    ) -> None:
        self.src = src
        self.dst = dst
        self.n_threads = n_threads
        self.include = include or []
        self.exclude = exclude or []
        self.pre_existing_files_on_dst: Optional[Set[str]] = None
        self.delete_after_copy = delete_after_copy
        self.remove_if_not_in_source = remove_if_not_in_source
        self._logger = logger

    def _skip(self, dirname: str, fn: str, src: bool = True) -> bool:
        """We skip the file if the pattern is in exclude *and not* in include.
        We also skip if the file was pre-existing on the destination directory,
        before the sync started.
        """

        def _skip_test(fn: str) -> bool:
            return any(fnmatch(fn, pat) for pat in self.exclude) and not any(
                fnmatch(fn, pat) for pat in self.include
            )

        relative_fn = self._relative_to_src if src else self._relative_to_dst

        assert self.pre_existing_files_on_dst is not None
        skip_if_preexisting_in_dest = (
            not src and os.path.join(dirname, fn) in self.pre_existing_files_on_dst
        )

        return _skip_test(fn) or _skip_test(relative_fn(dirname, fn)) or skip_if_preexisting_in_dest

    def _needs_update(self, fn1: str, fn2: str) -> bool:
        def _cmptimestamps(filest1: os.stat_result, filest2: os.stat_result) -> bool:
            """Compare time stamps of two files and return True
            if file1 (source) is more recent than file2 (target)"""

            mtime_cmp = int((filest1.st_mtime - filest2.st_mtime) * 1000) > 0
            return mtime_cmp or int((filest1.st_ctime - filest2.st_mtime) * 1000) > 0

        st1 = os.stat(fn1)
        st2 = os.stat(fn2)
        return bool(_cmptimestamps(st1, st2))

    @staticmethod
    def _copy(src: str, dst: str, delete_after_copy: bool) -> None:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)

        if delete_after_copy:
            os.unlink(src)

    @staticmethod
    def _delete(dst: str) -> None:
        if os.path.isfile(dst):
            try:
                try:
                    os.remove(dst)
                except PermissionError:
                    import stat

                    os.chmod(dst, stat.S_IWRITE)
                    os.remove(dst)
            except OSError:
                pass
        elif os.path.isdir(dst):
            try:
                shutil.rmtree(dst, True)
            except shutil.Error:
                pass

    def _relative_to_src(self, dirpath: str, fn: str) -> str:
        return os.path.relpath(os.path.join(dirpath, fn), self.src)

    def _relative_to_dst(self, dirpath: str, fn: str) -> str:
        return os.path.relpath(os.path.join(dirpath, fn), self.dst)

    def scan_preexisting_on_destination(self) -> None:
        """Scans pre-existing files on destination, so that they are not purged,
        if they are not present in the source.
        """
        self.pre_existing_files_on_dst = set()

        for dirpath, _, filenames in os.walk(self.dst, topdown=True):
            for fn in filenames:
                dst_fn = os.path.join(dirpath, fn)
                self.pre_existing_files_on_dst.add(dst_fn)

    def sync(self) -> None:
        if not os.path.isdir(self.src):
            return

        # first list existing files on destination, if they are already there, then
        # they must not be purged.
        if self.pre_existing_files_on_dst is None:
            self.scan_preexisting_on_destination()

        with ThreadPoolExecutor(self.n_threads) as tpe:
            # walk source directory and copy files not present in the destination directory
            futures = []
            for dirpath, dirnames, filenames in os.walk(self.src, topdown=True):
                skippable = [d for d in dirnames if self._skip(dirpath, d)]
                for d in skippable:
                    dirnames.remove(d)
                for fn in filenames:
                    if self._skip(dirpath, fn):
                        continue
                    src_fn = os.path.join(dirpath, fn)
                    dst_fn = os.path.join(self.dst, self._relative_to_src(dirpath, fn))
                    if os.path.exists(dst_fn) and not self._needs_update(src_fn, dst_fn):
                        continue
                    self._logger.debug("Syncing %r to %r", src_fn, dst_fn)
                    futures.append(tpe.submit(self._copy, src_fn, dst_fn, self.delete_after_copy))
            for future in as_completed(futures):
                future.result()  # check for exceptions

        # walk destination directory and remove files not present in the source directory
        # we only execute this if we don't delete after copy (otw all files would be deleted
        # from target just after being uploaded)
        if not self.delete_after_copy and self.remove_if_not_in_source:
            with ThreadPoolExecutor(self.n_threads) as tpe:
                futures = []
                for dirpath, dirnames, filenames in os.walk(self.dst, topdown=True):
                    skippable = [d for d in dirnames if self._skip(dirpath, d, src=False)]

                    for d in skippable:
                        dirnames.remove(d)

                    for fn in filenames:
                        if self._skip(dirpath, fn, src=False):
                            continue

                        dst_fn = os.path.join(dirpath, fn)
                        src_fn = os.path.join(self.src, self._relative_to_dst(dirpath, fn))
                        # we only check whether it exists. if it doesn't exist, we remove,
                        # if it's different then the above loop will take care of the update
                        if os.path.exists(src_fn):
                            continue

                        self._logger.info("Removing %r", dst_fn)
                        futures.append(tpe.submit(self._delete, dst_fn))
                for future in as_completed(futures):
                    future.result()  # check for exceptions


def _run(
    shutdown_event: synchronize.Event,
    flush_event: synchronize.Event,
    done_event: synchronize.Event,
    logging_queue: "queues.Queue[logging.LogRecord]",
    src: str,
    dst: str,
    freq: float,
    n_sync_threads: int,
    delete_after_copy: bool = False,
    remove_if_not_in_source: bool = True,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
) -> None:
    """
    subprocess that runs Syncer() in a loop and calls flush/shuts down depending on signals.
    """
    log_handler = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger("background_dirsync")
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    logger.info("Starting directory syncer from %r to %r, every %fs", src, dst, freq)
    syncer = Syncer(
        src,
        dst,
        n_threads=n_sync_threads,
        include=include,
        exclude=exclude,
        logger=logger,
        delete_after_copy=delete_after_copy,
        remove_if_not_in_source=remove_if_not_in_source,
    )
    while True:
        if flush_event.wait(freq):
            logger.info("Flush sync %r to %r", src, dst)
            flush_event.clear()
        try:
            syncer.sync()
        except Exception as exc:
            logger.warning("Failed to sync %r to %r: %s", src, dst, str(exc))
        finally:
            # signal to caller (eg flush()) that we're done
            done_event.set()
        if not flush_event.is_set() and shutdown_event.is_set():
            break

    logger.info("Clean shutdown of directory syncer from %r to %r", src, dst)


class BackgroundDirSync(ContextDecorator):
    """
    Synchronizes a directory to a destination directory
    """

    def __init__(
        self,
        src: str,
        dst: str,
        freq: int = 5,
        n_threads: int = 5,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        delete_after_copy: bool = False,
        remove_if_not_in_source: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.src = src
        self.dst = dst
        self.freq = freq
        self.n_sync_threads = n_threads
        self.delete_after_copy = delete_after_copy
        self.remove_if_not_in_source = remove_if_not_in_source
        self.sync_process: Optional[process.BaseProcess] = None
        self._logger = logger or logging.getLogger("background_dirsync")
        self.include = (
            os.environ.get("AMLT_DIRSYNC_INCLUDE", "").split() if include is None else include
        )
        self.exclude = list(
            os.environ.get("AMLT_DIRSYNC_EXCLUDE", "").split() if exclude is None else exclude
        )
        self.exclude.append(FLUSH_FILENAME)

        self.mp_ctx = mp.get_context("spawn")
        self.shutdown_event = self.mp_ctx.Event()
        self.log_shutdown_event = self.mp_ctx.Event()
        self.flush_event = self.mp_ctx.Event()
        self.done_event = self.mp_ctx.Event()

        # set up logging from subprocess
        self.log_queue = self.mp_ctx.Queue(-1)

    def __enter__(self) -> "BackgroundDirSync":
        self.start()
        return self

    def __exit__(self, *exc: Any) -> Literal[False]:
        self.shutdown()
        return False

    def flush(self, wait: bool = True) -> None:
        if not self.sync_process:
            return
        self.done_event.clear()
        self.flush_event.set()
        if wait:
            self.done_event.wait()

    def _log_listener(
        self,
        shutdown_event: synchronize.Event,
        log_queue: "queues.Queue[logging.LogRecord]",
    ) -> None:
        """Runs in a separate thread and passes log info from subprocess to logger."""
        shutdown = False
        while not shutdown:
            if shutdown_event.wait(1):
                shutdown = True
            while not log_queue.empty():
                record = log_queue.get()
                logger = logging.getLogger(record.name)
                logger.handle(record)

    def _flush_listener(
        self,
        shutdown_event: synchronize.Event,
        flush_event: synchronize.Event,
        done_event: synchronize.Event,
    ) -> None:
        """Flushes the uploader whenever a file FLUSH_FILENAME is created in source dir.
        Deletes the file once done.
        """
        watch_file = os.path.join(self.src, FLUSH_FILENAME)
        while True:
            if shutdown_event.wait(1):
                break
            if os.path.exists(watch_file):
                self._logger.info(
                    "Flush triggered since %s was found in %s.",
                    FLUSH_FILENAME,
                    self.src,
                )
                done_event.clear()
                flush_event.set()
                done_event.wait()
                os.unlink(watch_file)

    def start(self) -> None:
        assert self.sync_process is None, "BackgroundDirSync has already started"

        self.logging_thread = Thread(
            target=self._log_listener, args=(self.log_shutdown_event, self.log_queue)
        )
        self.logging_thread.daemon = True
        self.logging_thread.start()

        self.flushing_thread = Thread(
            target=self._flush_listener,
            args=(self.log_shutdown_event, self.flush_event, self.done_event),
        )
        self.flushing_thread.daemon = True
        self.flushing_thread.start()

        self.sync_process = self.mp_ctx.Process(
            target=_run,
            args=(
                self.shutdown_event,
                self.flush_event,
                self.done_event,
                self.log_queue,
                self.src,
                self.dst,
                self.freq,
                self.n_sync_threads,
                self.delete_after_copy,
                self.remove_if_not_in_source,
                self.include,
                self.exclude,
            ),
        )
        self.sync_process.daemon = True
        self.sync_process.start()
        atexit.register(self.shutdown)

    def shutdown(self) -> None:
        if not self.sync_process:
            return

        atexit.unregister(self.shutdown)

        # one final flush
        self.flush_event.set()
        self.shutdown_event.set()
        self.sync_process.join()

        self.log_shutdown_event.set()
        self.logging_thread.join()
        self.flushing_thread.join()

        self.sync_process = None
