import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.workforce import Workforce

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
        storage_account: The storage account containing the queue.
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
        self._states: list[Workforce.State] | None = None

        # Suppress verbose logging from Azure libraries
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.ai.ml").setLevel(logging.ERROR)

    @property
    def states(self) -> list[Workforce.State]:
        """
        Get the current states of all workforces.

        Returns:
            A list of Workforce.State objects for all workforces.
        """
        if self.use_lazy_states and self._states is not None:
            return self._states

        self._states = [workforce.get_current_state() for workforce in self.workforces]
        return self._states

    async def determine_number_of_workers(self) -> int:
        """
        Determine the optimal number of workers based on queue size and current state.

        Returns:
            The number of workers to scale to.
        """
        async with JobQ.from_storage_queue(
            self.queue_name,
            storage_account=self.storage_account,
            credential=self.credential,
        ) as jobq:
            queue_size = await jobq.get_approximate_size()
            LOG.info(f"Queue size: {queue_size}")

        if queue_size == 0:
            return 0
        else:
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

    async def run(self, scale_to_zero=False) -> bool:
        """
        Run the workforce scaling operation.

        This method determines how many workers should run and either scales up or down
        based on the queue size and current state.

        Args:
            scale_to_zero: If True, scale all workforces to zero.

        Returns:
            True if the scaling operation was successful, False otherwise.
        """
        currently_running = sum([s.num_running for s in self.states])
        currently_queued = sum([s.num_queued for s in self.states])
        LOG.info(
            f"Running {currently_running} workers and queued {currently_queued} workers on all workforces for queue {self.queue_name}."
        )
        nb_scale_to = await asyncio.gather(self.determine_number_of_workers())
        total_current = currently_running + currently_queued

        # Handle scale to zero case - do this in parallel for efficiency
        if scale_to_zero:
            with ThreadPoolExecutor() as executor:

                def scale_to_helper(workforce: Workforce, num_workers: int):
                    workforce.scale_to(num_workers, with_layoffs=True)
                    LOG.info(f"Scaled {workforce} to {num_workers}.")

                executor.map(scale_to_helper, self.workforces, [0] * len(self.workforces))
            return True

        # Handle scaling
        total_to_hire = nb_scale_to[0] - total_current

        LOG.info(f"Scaling to {max(nb_scale_to[0], 0)}, need to hire {total_to_hire}.")

        if total_to_hire == 0:
            return True
        if total_to_hire < 0:
            LOG.info(
                f"Need to scale down {total_to_hire} workers, this is currently not implemented."
            )
            return True

        # Calculate available capacity for each workforce
        available_for_hire_list = []
        for workforce, current_state in zip(self.workforces, self.states, strict=True):
            try:
                cluster_info = workforce.get_compute_infos().properties.properties
                max_node_count = cluster_info.scaleSettings["maxNodeCount"]
            except (RuntimeError, AttributeError, KeyError):
                # TODO: implement logic for singularity
                max_node_count = 200

                class ClusterDummy:
                    targetNodeCount = 0

                cluster_info = ClusterDummy()  # type: ignore

            nb_available = max_node_count - cluster_info.targetNodeCount  # type: ignore[operator]
            available_for_hire = nb_available - current_state.num_queued  # type: ignore[operator]
            available_for_hire_list.append(available_for_hire)

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

        # Hire workers in parallel for better performance
        with ThreadPoolExecutor() as executor:

            def hiring_helper(workforce: Workforce, num_workers_to_hire: int):
                if num_workers_to_hire > 0:
                    LOG.info(f"Hiring {num_workers_to_hire} workers on cluster {workforce}.")
                    workforce.hire(num_workers_to_hire)

            executor.map(hiring_helper, self.workforces, hiring_distribution)

        return sum(hiring_distribution) == total_to_hire

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
