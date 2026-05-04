# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.workforce import Workforce

LOG = logging.getLogger(__name__)

# Maximum number of workers to scale up to, API does not support paging over 50000
MAX_WORKERS_LIMIT = 50000

# Cap for the across-regions read-only thread pool. The per-region calls
# (``get_current_state``, ``get_available_to_hire``, ``get_detailed_state``)
# are network-bound HTTPS GETs against AML/ARM, not CPU-bound, so the right
# ceiling is shared-endpoint throttling headroom rather than ``os.cpu_count``.
_MAX_REGION_PARALLELISM = 32


def _region_parallelism(n: int) -> int:
    """Pick a thread count for across-regions read-only fan-out.

    Sequential below the threshold to avoid thread-pool overhead on small
    fleets; otherwise ``n // 5 + 1`` (e.g. 6 -> 2, 11 -> 3, 84 -> 17), capped
    at ``_MAX_REGION_PARALLELISM`` as a sanity guard.
    """
    if n <= 5:
        return 1
    return min(n // 5 + 1, _MAX_REGION_PARALLELISM)


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
        batched_delay_in_hiring: When True (default), hires are dispatched via
            ``Workforce.parallel_hire_in_batches`` (batches of 512 with a 10 s
            sleep between batches) instead of a single ``parallel_hire`` burst.
            Avoids overloading the AzureML MFE / container registry on large
            multi-region hires.
    """

    def __init__(
        self,
        queue_name: str,
        storage_account: str,
        workforces: list[Workforce],
        num_workers: int = 1,
        max_num_workers: int = MAX_WORKERS_LIMIT,
        use_lazy_states: bool = False,
        batched_delay_in_hiring: bool = True,
        parallel_region_reads: bool = False,
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
            batched_delay_in_hiring: When True (default), dispatch hires via
                ``Workforce.parallel_hire_in_batches`` to avoid MFE / container
                registry throttling on large hires.
            parallel_region_reads: When True, fan out the read-only per-region
                phases (``get_current_state``, ``get_available_to_hire``, and
                the ``get_detailed_state`` discovery half of the resume sweep)
                across a thread pool sized via :func:`_region_parallelism`.
                The actual ``parallel_resume`` / ``parallel_hire`` /
                ``parallel_lay_off`` writer bursts remain outer-sequential to
                avoid amplifying MFE write pressure (each already runs an
                8-thread inner pool per region). Defaults to False.
        """
        self.workforces = workforces
        self.num_workers = num_workers
        self.queue_name = queue_name
        self.storage_account = storage_account
        self.credential = get_token_credential()
        self.max_num_workers = max_num_workers
        self.use_lazy_states = use_lazy_states
        # When True, hires are dispatched via
        # Workforce.parallel_hire_in_batches (batches of 512 with a 10 s
        # sleep between batches) instead of a single parallel_hire burst.
        # This avoids overloading the AzureML MFE / container registry on
        # large hires.
        self.batched_delay_in_hiring = batched_delay_in_hiring
        self.parallel_region_reads = parallel_region_reads
        self._states: list[Workforce.State] | None = None
        # Per-workforce last-known-good state, keyed by id(workforce). Used
        # by the ``states`` property to keep a single transient failure in
        # one region from tanking the whole tick.
        self._last_good_state: dict[int, Workforce.State] = {}

        # Tick instrumentation: counter, scheduler-uptime anchor, and
        # last-tick wall-clock so the banner can show "since last".
        self._tick_count = 0
        self._scheduler_started_monotonic: float | None = None
        self._last_tick_end_monotonic: float | None = None
        # Phase timings populated by ``run`` for the closing summary line.
        self._phase_durations: dict[str, float] = {}

        # Suppress verbose logging from Azure libraries
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.ai.ml").setLevel(logging.ERROR)

    def _hire_on(self, workforce: Workforce, n: int) -> None:
        """Hire ``n`` workers on ``workforce`` honoring ``batched_delay_in_hiring``."""
        if self.batched_delay_in_hiring:
            workforce.parallel_hire_in_batches(n)
        else:
            workforce.parallel_hire(n)

    @staticmethod
    def _fmt_uptime(seconds: float) -> str:
        s = int(seconds)
        h, rem = divmod(s, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}h{m:02d}m{s:02d}s"
        if m:
            return f"{m}m{s:02d}s"
        return f"{s}s"

    def _map_regions(self, fn, label: str):
        """Run ``fn(idx, total, wf) -> (value, duration_s, ok: bool)`` over every region.

        Returns the list of ``value``s in input order. Owns:

        - sequential vs. pooled execution (gated by ``parallel_region_reads``
          and :func:`_region_parallelism`);
        - heartbeat progress lines for long phases (every ~25% completion);
        - aggregate rollup (slowest 3 regions, failure count, p50/max
          duration) at INFO; per-region detail is emitted at DEBUG by the
          caller's ``fn``.

        ``fn`` is responsible for its own try/except and DEBUG-level
        per-region logging; ``ok=False`` from ``fn`` increments the failure
        counter in the rollup but does not raise.
        """
        n = len(self.workforces)
        if n == 0:
            return []
        threads = _region_parallelism(n) if self.parallel_region_reads else 1
        pooled = threads > 1
        LOG.info(
            "%s: starting on %d region(s) (%s, %d thread%s).",
            label,
            n,
            "parallel" if pooled else "sequential",
            threads,
            "" if threads == 1 else "s",
        )

        # Heartbeat threshold: roughly every quarter of the fleet, but not
        # more often than every 10 regions. Quiet on small fleets.
        heartbeat_every = max(10, n // 4) if n >= 20 else n + 1

        results: list = [None] * n
        durations: list[float] = [0.0] * n
        oks: list[bool] = [False] * n
        completed = 0
        phase_start = time.monotonic()

        def _record(i: int, value, dur: float, ok: bool) -> None:
            nonlocal completed
            results[i] = value
            durations[i] = dur
            oks[i] = ok
            completed += 1
            if completed % heartbeat_every == 0 and completed < n:
                LOG.info(
                    "%s: progress %d/%d (%.0f%%) elapsed=%.1fs",
                    label,
                    completed,
                    n,
                    100.0 * completed / n,
                    time.monotonic() - phase_start,
                )

        if not pooled:
            for idx, wf in enumerate(self.workforces, start=1):
                value, dur, ok = fn(idx, n, wf)
                _record(idx - 1, value, dur, ok)
        else:
            with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="mrw-region") as pool:
                future_to_i = {
                    pool.submit(fn, idx, n, wf): idx - 1
                    for idx, wf in enumerate(self.workforces, start=1)
                }
                for fut in as_completed(future_to_i):
                    i = future_to_i[fut]
                    value, dur, ok = fut.result()
                    _record(i, value, dur, ok)

        elapsed = time.monotonic() - phase_start
        # Rollup: failures + slowest 3 regions by duration.
        failures = sum(1 for ok in oks if not ok)
        ranked = sorted(
            ((durations[i], str(self.workforces[i])) for i in range(n)),
            reverse=True,
        )
        slowest = ", ".join(f"{name} ({dur:.1f}s)" for dur, name in ranked[:3])
        sorted_durs = sorted(durations)
        p50 = sorted_durs[n // 2]
        max_dur = sorted_durs[-1]
        LOG.info(
            "%s: done in %.1fs (failed %d/%d, p50=%.1fs max=%.1fs); slowest: %s",
            label,
            elapsed,
            failures,
            n,
            p50,
            max_dur,
            slowest or "n/a",
        )
        return results

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

        fetch_start = time.monotonic()

        def fetch_one(idx: int, total: int, wf: Workforce):
            region_start = time.monotonic()
            try:
                state = wf.get_current_state()
            except Exception as exc:
                cached = self._last_good_state.get(id(wf))
                dur = time.monotonic() - region_start
                if cached is not None:
                    LOG.warning(
                        "[%d/%d] %s: get_current_state failed (%s); using cached %s.",
                        idx,
                        total,
                        wf,
                        exc,
                        cached,
                    )
                    return cached, dur, False
                LOG.warning(
                    "[%d/%d] %s: get_current_state failed (%s) and no cache; "
                    "assuming zero workers.",
                    idx,
                    total,
                    wf,
                    exc,
                )
                return Workforce.State(num_pending=0, num_running=0), dur, False
            self._last_good_state[id(wf)] = state
            dur = time.monotonic() - region_start
            LOG.debug(
                "[%d/%d] %s: state running=%d pending=%d (%.1fs).",
                idx,
                total,
                wf,
                state.num_running,
                state.num_pending,
                dur,
            )
            return state, dur, True

        states = self._map_regions(fetch_one, "Fetching state")
        self._phase_durations["state"] = time.monotonic() - fetch_start
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
        # Tick banner: tick number, scheduler uptime, and gap since last tick.
        # Helps tell forever-mode progress from a hung loop at a glance.
        now = time.monotonic()
        if self._scheduler_started_monotonic is None:
            self._scheduler_started_monotonic = now
        self._tick_count += 1
        uptime = now - self._scheduler_started_monotonic
        since_last = (
            (now - self._last_tick_end_monotonic)
            if self._last_tick_end_monotonic is not None
            else 0.0
        )
        LOG.info(
            "═══ Tick #%d (uptime %s, since last %.1fs) ═══",
            self._tick_count,
            self._fmt_uptime(uptime),
            since_last,
        )
        # Reset per-tick instrumentation; ``states`` cache gets rebuilt below.
        self._phase_durations = {}
        self._states = None

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
            LOG.info(
                "Tick done (scale-to-zero) across %d region(s) in %.1fs.",
                len(self.workforces),
                time.monotonic() - tick_start,
            )
            self._last_tick_end_monotonic = time.monotonic()
            return True

        # Handle scaling
        total_to_hire = nb_scale_to[0] - total_current
        if manual_hire is not None:
            LOG.info(f"Manual hire set to {manual_hire}. Overwriting autoscaling.")
            total_to_hire = manual_hire
            self._apply_uniform_change(manual_hire)
            self._last_tick_end_monotonic = time.monotonic()
            return True

        LOG.info(f"Scaling to {max(nb_scale_to[0], 0)}, need to hire {total_to_hire}.")

        if total_to_hire == 0:
            self._last_tick_end_monotonic = time.monotonic()
            return True
        if total_to_hire < 0:
            if currently_pending > 0:
                layoff_distribution = self.layoff_queued_workers(total_to_layoff=-total_to_hire)
                if sum(layoff_distribution) == -total_to_hire:
                    self._last_tick_end_monotonic = time.monotonic()
                    return True
                # after stopping queued workers, we check if we still need to scale down
                total_to_hire += sum(layoff_distribution)

            if total_to_hire < 0:
                # TODO - for layoff of running workers, the new servicebus feature could be used to do graceful shutdown
                # this is not implemented yet
                LOG.info(
                    f"Need to scale down {-total_to_hire} workers, this is currently not implemented."
                )
                self._last_tick_end_monotonic = time.monotonic()
                return False

        # Calculate available capacity for each workforce
        capacity_start = time.monotonic()
        states_snapshot = self.states

        def query_one(idx: int, total: int, workforce: Workforce):
            current_state = states_snapshot[idx - 1]
            region_start = time.monotonic()
            try:
                available_for_hire = workforce.get_available_to_hire(current_state=current_state)
            except Exception as exc:
                dur = time.monotonic() - region_start
                LOG.warning(
                    "[%d/%d] %s: get_available_to_hire failed (%s); assuming 0 capacity.",
                    idx,
                    total,
                    workforce,
                    exc,
                )
                return 0, dur, False
            dur = time.monotonic() - region_start
            LOG.debug(
                "[%d/%d] %s: available=%d (%.1fs).",
                idx,
                total,
                workforce,
                available_for_hire,
                dur,
            )
            return available_for_hire, dur, True

        available_for_hire_list = self._map_regions(query_one, "Querying capacity")
        self._phase_durations["capacity"] = time.monotonic() - capacity_start
        LOG.info(
            "Total available capacity across regions: %d.",
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

        # Sequential across regions; each region uses parallel_hire
        # (8-thread pool) internally.
        hire_start = time.monotonic()
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
                self._hire_on(workforce, n)
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
        self._phase_durations["hire"] = time.monotonic() - hire_start

        # Resume any paused workers in every region at the end of each tick.
        # parallel_resume is a no-op when nothing is paused, so calling it
        # unconditionally is cheap (one list_jobs per region) and keeps the
        # running pool from bleeding away on Singularity (max-exec-time).
        total_resumed = self._resume_all_regions()

        tick_elapsed = time.monotonic() - tick_start
        total_hired = sum(hiring_distribution)
        # Per-tick summary: one line containing the whole picture.
        LOG.info(
            "Tick #%d summary: want=%d have=%d (run=%d pend=%d) hired=%d/%d resumed=%d "
            "regions=%d elapsed=%.1fs",
            self._tick_count,
            max(nb_scale_to[0], 0),
            total_current,
            currently_running,
            currently_pending,
            total_hired,
            total_to_hire,
            total_resumed,
            len(self.workforces),
            tick_elapsed,
        )
        # Phase timing summary: lets you see at a glance which phase dominates.
        LOG.info(
            "Tick #%d phases: %s",
            self._tick_count,
            " ".join(f"{k}={v:.1f}s" for k, v in self._phase_durations.items()),
        )
        self._last_tick_end_monotonic = time.monotonic()
        return total_hired == total_to_hire

    def _apply_uniform_change(self, delta: int) -> None:
        """Apply the same hire/lay-off delta to every region.

        Used by ``manual_hire``: positive delta hires ``delta`` on each region,
        negative delta lays off ``-delta`` on each region.
        """
        if delta == 0:
            return

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
                if delta > 0:
                    self._hire_on(wf, delta)
                else:
                    wf.parallel_lay_off(-delta)
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

    def _resume_all_regions(self) -> int:
        """Resume paused workers in every region; returns total resumed.

        Discovery (``get_detailed_state``, a cheap read) is fanned out across
        regions when ``parallel_region_reads`` is on; the actual
        ``parallel_resume`` writer burst stays outer-sequential because it
        already runs an 8-thread inner pool per region, and we don't want to
        multiply MFE write pressure by the outer thread count.
        """
        total_regions = len(self.workforces)

        discovery_start = time.monotonic()

        def discover(idx: int, total: int, workforce: Workforce):
            region_start = time.monotonic()
            try:
                state = workforce.get_detailed_state()
            except Exception:
                dur = time.monotonic() - region_start
                LOG.exception(
                    "[%d/%d] %s: failed to query paused workers; skipping resume.",
                    idx,
                    total,
                    workforce,
                )
                return None, dur, False
            dur = time.monotonic() - region_start
            return state.num_paused, dur, True

        paused_counts = self._map_regions(discover, "Resume discovery")
        self._phase_durations["resume_disc"] = time.monotonic() - discovery_start

        resume_start = time.monotonic()
        total_resumed = 0
        active = [
            (idx, wf, n)
            for idx, (wf, n) in enumerate(zip(self.workforces, paused_counts, strict=True), start=1)
            if n is not None and n > 0
        ]
        for idx, workforce, num_paused in active:
            LOG.info(
                "[%d/%d] %s: resuming %d paused workers.",
                idx,
                total_regions,
                workforce,
                num_paused,
            )
            region_start = time.monotonic()
            try:
                workforce.parallel_resume(num_paused)
            except Exception:
                LOG.exception("resume of paused workers failed on %s.", workforce)
            else:
                total_resumed += num_paused
                LOG.info(
                    "[%d/%d] %s: resume complete in %.1fs.",
                    idx,
                    total_regions,
                    workforce,
                    time.monotonic() - region_start,
                )
        self._phase_durations["resume"] = time.monotonic() - resume_start
        if total_resumed:
            LOG.info("Resumed %d paused workers across %d region(s).", total_resumed, total_regions)
        return total_resumed

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
