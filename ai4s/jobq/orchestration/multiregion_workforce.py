# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Literal

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.workforce import Workforce

ParallelStrategy = Literal["worker_parallel", "region_parallel"]

LOG = logging.getLogger(__name__)

# Maximum number of workers to scale up to, API does not support paging over 50000
MAX_WORKERS_LIMIT = 50000


class MultiRegionWorkforce:
    """
    A class that manages workforces across multiple regions for scalable job processing.

    This implementation provides parallel hiring and scaling across multiple clusters or regions
    to improve efficiency when managing large numbers of workers.

    Important: Each Workforce instance in the provided list must use a unique experiment_name.
    This is because Workforce.get_current_state() counts all jobs within an experiment, which
    can lead to incorrect worker count calculations if multiple workforce instances share the
    same experiment name.

    Attributes:
        workforces: A list of Workforce objects to manage across regions.
        num_workers: Number of workers per job.
        queue_name: The name of the queue to process.
        storage_account: The storage account containing the queue or the servicebus, in the format sb://SERVICEBUS_NAMESPACE.
        credential: Azure credential for authentication.
        max_num_workers: Maximum number of workers to scale up to (defaults to MAX_WORKERS_LIMIT).
        use_lazy_states: Whether to cache workforce states between calls.
        with_layoffs: Whether to allow layoffs when scaling down.
    """

    def __init__(
        self,
        queue_name: str,
        storage_account: str,
        workforces: list[Workforce],
        num_workers: int = 1,
        max_num_workers: int = MAX_WORKERS_LIMIT,
        use_lazy_states: bool = False,
        parallel_strategy: ParallelStrategy = "worker_parallel",
    ):
        """
        Initialize the MultiRegionWorkforce.

        Args:
            queue_name: Name of the queue to process.
            storage_account: The storage account containing the queue.
            workforces: List of Workforce objects to manage across regions. Each workforce
                        must have a unique experiment_name to ensure correct worker counting.
            num_workers: Number of workers per job (defaults to 1).
            max_num_workers: Maximum number of workers to scale up to (defaults to MAX_WORKERS_LIMIT).
            use_lazy_states: Whether to cache workforce states between calls.
        """
        self.workforces = workforces
        self.num_workers = num_workers
        self.queue_name = queue_name
        self.storage_account = storage_account
        self.credential = get_token_credential()
        self.max_num_workers = max_num_workers
        self.use_lazy_states = use_lazy_states
        # worker_parallel (default): loop regions sequentially, each region uses
        #   Workforce.parallel_hire / parallel_lay_off (8 threads internally).
        #   Well-suited to uneven distributions: a region that needs 5 hires does not
        #   block one that needs 500.
        # region_parallel (legacy): fan out across regions via a ThreadPoolExecutor,
        #   each region uses the sequential Workforce.hire / lay_off.
        if parallel_strategy not in ("worker_parallel", "region_parallel"):
            raise ValueError(
                f"Invalid parallel_strategy {parallel_strategy!r}; "
                "expected 'worker_parallel' or 'region_parallel'."
            )
        self.parallel_strategy: ParallelStrategy = parallel_strategy
        self._states: list[Workforce.State] | None = None
        # Per-workforce last-known-good state, keyed by id(workforce). Used
        # by the ``states`` property to keep a single transient failure in
        # one region from tanking the whole tick.
        self._last_good_state: dict[int, Workforce.State] = {}

        # Suppress verbose logging from Azure libraries
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.ai.ml").setLevel(logging.ERROR)

    @property
    def states(self) -> list[Workforce.State]:
        """
        Get the current states of all workforces.

        Per-workforce failure is tolerated: if ``get_current_state`` raises
        (e.g. a transient 5xx from AML that outlasted ``list_jobs``'s own
        retries), we fall back to the most recent successful reading for
        that workforce, or to a zero state if none exists yet. The run
        proceeds with the best estimate available rather than aborting.

        Returns:
            A list of Workforce.State objects for all workforces.
        """
        if self.use_lazy_states and self._states is not None:
            return self._states

        LOG.info("Fetching state for %d region(s)...", len(self.workforces))
        fetch_start = time.monotonic()
        states: list[Workforce.State] = []
        total_regions = len(self.workforces)
        for idx, wf in enumerate(self.workforces, start=1):
            region_start = time.monotonic()
            try:
                state = wf.get_current_state()
            except Exception as exc:
                cached = self._last_good_state.get(id(wf))
                if cached is not None:
                    LOG.warning(
                        "[%d/%d] %s: get_current_state failed (%s); using cached %s.",
                        idx,
                        total_regions,
                        wf,
                        exc,
                        cached,
                    )
                    state = cached
                else:
                    LOG.warning(
                        "[%d/%d] %s: get_current_state failed (%s) and no cache; "
                        "assuming zero workers.",
                        idx,
                        total_regions,
                        wf,
                        exc,
                    )
                    state = Workforce.State(num_pending=0, num_running=0)
            else:
                self._last_good_state[id(wf)] = state
                LOG.info(
                    "[%d/%d] %s: state running=%d pending=%d (%.1fs).",
                    idx,
                    total_regions,
                    wf,
                    state.num_running,
                    state.num_pending,
                    time.monotonic() - region_start,
                )
            states.append(state)

        LOG.info(
            "State fetched for %d region(s) in %.1fs.",
            total_regions,
            time.monotonic() - fetch_start,
        )
        self._states = states
        return self._states

    async def determine_number_of_workers(self) -> int:
        """
        Determine the optimal number of workers based on queue size and current state.

        Returns:
            The number of workers to scale to.
        """
        if self.storage_account.startswith("sb://"):
            # backend is a servicebus queue
            async with JobQ.from_service_bus(
                self.queue_name,
                fqns=f"{self.storage_account[5:]}.servicebus.windows.net",
                credential=self.credential,
            ) as jobq:
                queue_size = await jobq.get_approximate_size()
        else:
            async with JobQ.from_storage_queue(
                self.queue_name,
                storage_account=self.storage_account,
                credential=self.credential,
            ) as jobq:
                queue_size = await jobq.get_approximate_size()

        LOG.info(f"Queue size: {queue_size}")

        if queue_size == 0:
            return 0
        # Get current running workers
        num_running_workers = sum([s.num_running for s in self.states])

        # Log a warning if we detect duplicate experiment names
        experiment_names = [wf._experiment_name for wf in self.workforces]
        if len(experiment_names) != len(set(experiment_names)):
            LOG.warning(
                "Duplicate experiment names detected in workforces. This will cause incorrect "
                "worker counting since Workforce.get_current_state() counts all jobs within an experiment."
            )

        # We don't want to scale too fast
        max_number_after_scaling = (1 + num_running_workers) * 10

        # Now we choose a minimum workforce size based on the queue size.
        if queue_size // self.num_workers > 10000:
            scale_to = min(max_number_after_scaling, 1000)
        elif queue_size // self.num_workers > 1000:
            scale_to = min(max_number_after_scaling, 400)
        elif queue_size // self.num_workers > 100:
            scale_to = min(max_number_after_scaling, 50)
        elif queue_size // self.num_workers > 20:
            scale_to = min(max_number_after_scaling, 15)
        elif queue_size // self.num_workers > 10:
            scale_to = min(max_number_after_scaling, 5)
        else:
            scale_to = min(max_number_after_scaling, 1)

        # We should not have more runners than the length of the queue or exceed the max limit
        scale_to = min(scale_to, queue_size, self.max_num_workers)
        return scale_to

    def layoff_queued_workers(self, total_to_layoff: int) -> list[int]:
        """Lays off queued workers up to total_to_layoff and returns the distribution of layoffs over the workforces.
        Args:
            total_to_layoff (int): The total number of workers to lay off.
        Returns:
            A list of integers representing the number of workers laid off from each workforce.
        """
        LOG.info(f"Stopping queued workers, total to stop: {total_to_layoff}.")
        # scale down by removing queued workers
        layoff_distribution: list[int] = [0] * len(self.workforces)
        avg_num_to_layoff = total_to_layoff // len(self.workforces)
        available_for_layoff_list = [s.num_pending for s in self.states]
        carry = 0

        # Sort by available capacity to better distribute workers
        for index, available_for_layoff in sorted(
            enumerate(available_for_layoff_list), key=lambda e: e[1]
        ):
            planned_to_layoff = min(
                avg_num_to_layoff + carry,
                available_for_layoff if available_for_layoff > 0 else 0,
                total_to_layoff - sum(layoff_distribution),
            )
            carry += avg_num_to_layoff - planned_to_layoff
            layoff_distribution[index] = planned_to_layoff

        if self.parallel_strategy == "worker_parallel":
            # Sequential across regions; each region uses parallel_lay_off
            # (8-thread pool) internally. Uneven distributions don't block
            # the whole call since small regions finish almost instantly.
            for workforce, n in zip(self.workforces, layoff_distribution, strict=True):
                if n <= 0:
                    continue
                LOG.info(f"Stopping {n} workers on cluster {workforce}.")
                try:
                    workforce.parallel_lay_off(n)
                except Exception:
                    LOG.exception("layoff of queued workers failed on %s.", workforce)
        else:  # region_parallel (legacy)
            with ThreadPoolExecutor() as executor:

                def layoff_helper(workforce: Workforce, num_workers_to_layoff: int):
                    if num_workers_to_layoff > 0:
                        LOG.info(
                            f"Stopping {num_workers_to_layoff} workers on cluster {workforce}."
                        )
                        workforce.lay_off(num_workers_to_layoff)

                layoff_futures: list[Future[Any]] = [
                    executor.submit(layoff_helper, workforce, num_workers_to_hire)
                    for workforce, num_workers_to_hire in zip(
                        self.workforces, layoff_distribution, strict=True
                    )
                ]
                for future in as_completed(layoff_futures):
                    try:
                        future.result()
                    except Exception:
                        LOG.exception("layoff of queued workers failed.")

        return layoff_distribution

    async def run(self, scale_to_zero=False, manual_hire: int | None = None) -> bool:
        """
        Run the workforce scaling operation.

        This method determines how many workers should run and either scales up or down
        based on the queue size and current state.

        Args:
            scale_to_zero: If True, scale all workforces to zero.
            manual_hire: Number of workers to hire. Overwrites autoscaling.

        Returns:
            True if the scaling operation was successful, False otherwise.
        """
        currently_running = sum([s.num_running for s in self.states])
        currently_pending = sum([s.num_pending for s in self.states])
        LOG.info(
            "Tick start on %d region(s) for queue %s: %d running, %d pending (total %d).",
            len(self.workforces),
            self.queue_name,
            currently_running,
            currently_pending,
            currently_running + currently_pending,
        )
        tick_start = time.monotonic()
        nb_scale_to = await asyncio.gather(self.determine_number_of_workers())
        total_current = currently_running + currently_pending

        # Handle scale to zero case
        if scale_to_zero:
            if self.parallel_strategy == "worker_parallel":
                # Loop regions sequentially; lay off every active worker in each
                # region via parallel_lay_off (8 threads internally).
                total_regions = len(self.workforces)
                for idx, (workforce, state) in enumerate(
                    zip(self.workforces, self.states, strict=True), start=1
                ):
                    total = state.num_running + state.num_pending
                    if total <= 0:
                        LOG.info(
                            "[%d/%d] %s: already at 0, skipping.",
                            idx,
                            total_regions,
                            workforce,
                        )
                        continue
                    LOG.info(
                        "[%d/%d] %s: laying off %d workers.",
                        idx,
                        total_regions,
                        workforce,
                        total,
                    )
                    region_start = time.monotonic()
                    try:
                        workforce.parallel_lay_off(total)
                    except Exception:
                        LOG.exception("scaling down of workers failed on %s.", workforce)
                    else:
                        LOG.info(
                            "[%d/%d] %s: lay-off complete in %.1fs.",
                            idx,
                            total_regions,
                            workforce,
                            time.monotonic() - region_start,
                        )
            else:  # region_parallel (legacy)
                with ThreadPoolExecutor() as executor:

                    def scale_to_helper(workforce: Workforce, num_workers: int):
                        workforce.scale_to(num_workers, with_layoffs=True)
                        LOG.info(f"Scaled {workforce} to {num_workers}.")

                    scale_to_futures: list[Future[Any]] = [
                        executor.submit(scale_to_helper, workforce, num_workers_to_hire)
                        for workforce, num_workers_to_hire in zip(
                            self.workforces, [0] * len(self.workforces), strict=True
                        )
                    ]
                    for future in as_completed(scale_to_futures):
                        try:
                            future.result()
                        except Exception:
                            LOG.exception("scaling down of workers failed.")
            LOG.info(
                "Tick done (scale-to-zero) across %d region(s) in %.1fs.",
                len(self.workforces),
                time.monotonic() - tick_start,
            )
            return True

        # Handle scaling
        total_to_hire = nb_scale_to[0] - total_current
        if manual_hire is not None:
            LOG.info(f"Manual hire set to {manual_hire}. Overwriting autoscaling.")
            total_to_hire = manual_hire
            self._apply_uniform_change(manual_hire)
            return True

        LOG.info(f"Scaling to {max(nb_scale_to[0], 0)}, need to hire {total_to_hire}.")

        if total_to_hire == 0:
            return True
        if total_to_hire < 0:
            if currently_pending > 0:
                layoff_distribution = self.layoff_queued_workers(total_to_layoff=-total_to_hire)
                if sum(layoff_distribution) == -total_to_hire:
                    return True
                # after stopping queued workers, we check if we still need to scale down
                total_to_hire += sum(layoff_distribution)

            if total_to_hire < 0:
                # TODO - for layoff of running workers, the new servicebus feature could be used to do graceful shutdown
                # this is not implemented yet
                LOG.info(
                    f"Need to scale down {-total_to_hire} workers, this is currently not implemented."
                )
                return False

        # Calculate available capacity for each workforce
        LOG.info("Querying available hiring capacity for %d region(s)...", len(self.workforces))
        capacity_start = time.monotonic()
        available_for_hire_list = []
        total_regions = len(self.workforces)
        for idx, (workforce, current_state) in enumerate(
            zip(self.workforces, self.states, strict=True), start=1
        ):
            region_start = time.monotonic()
            try:
                available_for_hire = workforce.get_available_to_hire(current_state=current_state)
            except Exception as exc:
                LOG.warning(
                    "[%d/%d] %s: get_available_to_hire failed (%s); assuming 0 capacity.",
                    idx,
                    total_regions,
                    workforce,
                    exc,
                )
                available_for_hire = 0
            else:
                LOG.info(
                    "[%d/%d] %s: available=%d (%.1fs).",
                    idx,
                    total_regions,
                    workforce,
                    available_for_hire,
                    time.monotonic() - region_start,
                )
            available_for_hire_list.append(available_for_hire)
        LOG.info(
            "Capacity query for %d region(s) done in %.1fs (total available %d).",
            total_regions,
            time.monotonic() - capacity_start,
            sum(max(0, x) for x in available_for_hire_list),
        )

        # Distribute hiring across workforces
        hiring_distribution: list[int] = [0] * len(self.workforces)
        avg_num_to_hire = 1 + (total_to_hire // len(self.workforces)) if self.workforces else 0
        carry = 0

        # Sort by available capacity to better distribute workers
        for index, available_for_hire in sorted(
            enumerate(available_for_hire_list), key=lambda e: e[1]
        ):
            planned_to_hire = min(
                avg_num_to_hire + carry,
                available_for_hire if available_for_hire > 0 else 0,
                total_to_hire - sum(hiring_distribution),
            )
            carry += avg_num_to_hire - planned_to_hire
            hiring_distribution[index] = planned_to_hire

        if sum(hiring_distribution) != total_to_hire:
            LOG.info(
                f"Not enough available workers found on clusters {self.workforces}. Scaling up {sum(hiring_distribution)}, but should have scaled {total_to_hire}."
            )

        if self.parallel_strategy == "worker_parallel":
            # Sequential across regions; each region uses parallel_hire
            # (8-thread pool) internally.
            active = [
                (wf, n) for wf, n in zip(self.workforces, hiring_distribution, strict=True) if n > 0
            ]
            total_regions = len(active)
            for idx, (workforce, n) in enumerate(active, start=1):
                LOG.info(
                    "[%d/%d] %s: hiring %d workers.",
                    idx,
                    total_regions,
                    workforce,
                    n,
                )
                region_start = time.monotonic()
                try:
                    workforce.parallel_hire(n)
                except Exception:
                    LOG.exception("hiring of workers failed on %s.", workforce)
                else:
                    LOG.info(
                        "[%d/%d] %s: hire complete in %.1fs.",
                        idx,
                        total_regions,
                        workforce,
                        time.monotonic() - region_start,
                    )
        else:  # region_parallel (legacy)
            with ThreadPoolExecutor() as executor:

                def hiring_helper(workforce: Workforce, num_workers_to_hire: int):
                    if num_workers_to_hire > 0:
                        LOG.info(f"Hiring {num_workers_to_hire} workers on cluster {workforce}.")
                        workforce.hire(num_workers_to_hire)

                hiring_futures: list[Future[Any]] = [
                    executor.submit(hiring_helper, workforce, num_workers_to_hire)
                    for workforce, num_workers_to_hire in zip(
                        self.workforces, hiring_distribution, strict=True
                    )
                ]
                for future in as_completed(hiring_futures):
                    try:
                        future.result()
                    except Exception:
                        LOG.exception("hiring of workers failed.")

        # Resume any paused workers in every region at the end of each tick.
        # parallel_resume is a no-op when nothing is paused, so calling it
        # unconditionally is cheap (one list_jobs per region) and keeps the
        # running pool from bleeding away on Singularity (max-exec-time).
        self._resume_all_regions()

        LOG.info(
            "Tick done across %d region(s) in %.1fs: hired %d/%d workers.",
            len(self.workforces),
            time.monotonic() - tick_start,
            sum(hiring_distribution),
            total_to_hire,
        )
        return sum(hiring_distribution) == total_to_hire

    def _apply_uniform_change(self, delta: int) -> None:
        """Apply the same hire/lay-off delta to every region, honoring parallel_strategy.

        Used by ``manual_hire``: positive delta hires ``delta`` on each region,
        negative delta lays off ``-delta`` on each region.
        """
        if delta == 0:
            return

        def _apply(workforce: Workforce) -> None:
            if self.parallel_strategy == "worker_parallel":
                if delta > 0:
                    workforce.parallel_hire(delta)
                else:
                    workforce.parallel_lay_off(-delta)
            else:  # region_parallel dispatches per-region; inside a region use sequential
                if delta > 0:
                    workforce.hire(delta)
                else:
                    workforce.lay_off(-delta)

        if self.parallel_strategy == "worker_parallel":
            total_regions = len(self.workforces)
            verb = "hiring" if delta > 0 else "laying off"
            for idx, wf in enumerate(self.workforces, start=1):
                LOG.info(
                    "[%d/%d] %s: %s %d (manual).",
                    idx,
                    total_regions,
                    wf,
                    verb,
                    abs(delta),
                )
                region_start = time.monotonic()
                try:
                    _apply(wf)
                except Exception:
                    LOG.exception("manual hire/lay-off failed on %s.", wf)
                else:
                    LOG.info(
                        "[%d/%d] %s: done in %.1fs.",
                        idx,
                        total_regions,
                        wf,
                        time.monotonic() - region_start,
                    )
        else:  # region_parallel
            with ThreadPoolExecutor() as executor:
                futs = [executor.submit(_apply, wf) for wf in self.workforces]
                for fut in as_completed(futs):
                    try:
                        fut.result()
                    except Exception:
                        LOG.exception("manual hire/lay-off failed.")

    def _resume_all_regions(self) -> None:
        """Resume paused workers in every region (sequential across regions).

        Each region uses ``parallel_resume`` (8 threads internally). Called
        at the end of every :meth:`run` tick; cheap when nothing is paused.
        This always uses the worker_parallel pattern regardless of
        :attr:`parallel_strategy` because resume is best-effort per-job and
        benefits from intra-region threading even when region_parallel is
        selected for hire/lay-off.
        """
        total_regions = len(self.workforces)
        total_resumed = 0
        for idx, workforce in enumerate(self.workforces, start=1):
            try:
                state = workforce.get_detailed_state()
            except Exception:
                LOG.exception(
                    "[%d/%d] %s: failed to query paused workers; skipping resume.",
                    idx,
                    total_regions,
                    workforce,
                )
                continue
            if state.num_paused <= 0:
                continue
            LOG.info(
                "[%d/%d] %s: resuming %d paused workers.",
                idx,
                total_regions,
                workforce,
                state.num_paused,
            )
            region_start = time.monotonic()
            try:
                workforce.parallel_resume(state.num_paused)
            except Exception:
                LOG.exception("resume of paused workers failed on %s.", workforce)
            else:
                total_resumed += state.num_paused
                LOG.info(
                    "[%d/%d] %s: resume complete in %.1fs.",
                    idx,
                    total_regions,
                    workforce,
                    time.monotonic() - region_start,
                )
        if total_resumed:
            LOG.info("Resumed %d paused workers across %d region(s).", total_resumed, total_regions)

    async def run_forever(self, sleep_time: int = 60):
        """
        Run the workforce scaling operation in an infinite loop.

        This method continuously monitors the queue and scales the workforce accordingly,
        sleeping between iterations.

        Args:
            sleep_time: Number of seconds to sleep between iterations (defaults to 60).
        """
        while True:
            await self.run()
            await asyncio.sleep(sleep_time)
