# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""
This file contains tools for managing groups of workers executing the same task.
We call such a group a "workforce".

The main class is `Workforce`, which allows you to investigate the state of a workforce, and easily scale it to a desired size.

Example usage:

```python
from livdft.common.workforce import WorkForce
from azure.ai.ml import command

# Create a prototype job that will be used to create new workers.
worker_prototype = command(
    command="echo 'Hello, world!'",
    ...
)

workforce = Workforce("my-experiment", worker_prototype)

# Get the current state of the workforce.
state = workforce.get_current_state()

# Scale the workforce to >=10 workers.
workforce.scale_to(10, with_layoffs=False)
```

"""

import copy
import json
import logging
import os
import random
import string
import subprocess
import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from datetime import datetime
from typing import Any, Literal

import jwt
import requests
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Command
from azure.core.credentials import TokenCredential
from azure.core.exceptions import HttpResponseError
from dateutil.parser import parse as _parse_utc
from pydantic import BaseModel, ConfigDict
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

LOG = logging.getLogger(__name__)

Status = Literal[
    "Cancel requested",
    "Canceled",
    "Completed",
    "Failed",
    "Finalizing",
    "Not started",
    "Paused",
    "Preparing",
    "Queued",
    "Running",
    "Starting",
    "Succeeded",
    "Validating",
    "Waiting",
]


class AmlComputeInfoPropertiesProperties(BaseModel):
    model_config = ConfigDict(extra="forbid")
    vmSize: str
    vmPriority: str
    osType: str
    virtualMachineImage: str | None
    isolatedNetwork: bool
    subnet: dict[str, str] | None
    scaleSettings: dict[str, int | str]
    remoteLoginPortPublicAccess: str
    allocationState: str
    allocationStateTransitionTime: datetime
    errors: list | str | None
    currentNodeCount: int
    targetNodeCount: int
    nodeStateCounts: dict[str, int]
    enableBatchPrivateLink: bool
    propertyBag: dict
    enableNodePublicIp: bool | None = None


class AmlComputeInfoIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    tenantId: str | None = None
    userAssignedIdentities: dict[str, dict]


class AmlComputeInfoProperties(BaseModel):
    model_config = ConfigDict(extra="forbid")
    description: str | None
    createdOn: datetime
    modifiedOn: datetime
    computeType: str
    provisioningState: str
    resourceId: str | None
    computeLocation: str
    provisioningErrors: dict | None
    provisioningWarnings: dict
    isAttachedCompute: bool
    disableLocalAuth: bool
    properties: AmlComputeInfoPropertiesProperties


class AmlComputeInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    type: str
    name: str
    location: str
    tags: dict[str, str]
    identity: AmlComputeInfoIdentity
    properties: AmlComputeInfoProperties


class AmlExperiment(BaseModel):
    id: str
    subscription_id: str
    resource_group: str
    workspace: str


class AmlJob(BaseModel):
    experiment: AmlExperiment
    status: Status
    name: str
    start_time: datetime | None
    cluster: str
    error_msg: str | None = None
    metrics: dict[str, float]

    @property
    def url(self):
        wsid = f"/subscriptions/{self.experiment.subscription_id}/resourceGroups/{self.experiment.resource_group}/providers/Microsoft.MachineLearningServices/workspaces/{self.experiment.workspace}"
        return (
            f"https://ml.azure.com/experiments/id/{self.experiment.id}/runs/{self.name}?wsid={wsid}"
        )


def azcli(cmd, decode_json=True):
    try:
        out = subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        LOG.info(f"Command failed: {e.stderr}. trying again with login.")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        if client_id is not None:
            subprocess.check_output(["az", "login", "--identity", "--client-id", client_id])
            out = subprocess.check_output(cmd, shell=True)
        else:
            LOG.info("No AZURE_CLIENT_ID set, can not login with identity.")
            raise e
    return json.loads(out) if decode_json else out


class Workforce:
    """
    A workforce represents multiple workers that are executing the same task.
    Concretely, these workers are jobs in an AzureML experiment and in one cluster and they probably
    are 'Ai4s JobQ' workers.

    This class allows you to investigate the state of a workforce, and easily scale it to a desired
    size.

    This class is stateless. The state of the workforce is always queried from the AzureML service.
    """

    class State(BaseModel):
        """Lightweight workforce state for scaling decisions."""

        num_pending: int  # Queued + Preparing + Paused + Starting + Waiting
        num_running: int

    class DetailedState(BaseModel):
        """Extended workforce state with per-status breakdowns for monitoring."""

        num_running: int
        num_paused: int
        num_queued: int
        num_preparing: int
        num_starting: int
        num_waiting: int

        @property
        def num_pending(self) -> int:
            """Total non-running active jobs."""
            return (
                self.num_paused
                + self.num_queued
                + self.num_preparing
                + self.num_starting
                + self.num_waiting
            )

    def __init__(
        self,
        experiment_name: str,
        worker_prototype: Command,
        *,
        aml_client: MLClient,
        credential: TokenCredential,
        servicebus_resource_group: str | None = None,
        servicebus_namespace: str | None = None,
        servicebus_topic: str | None = None,
    ):
        self._job = worker_prototype
        self._experiment_name = experiment_name
        self._aml_client = aml_client
        self._credential = credential
        self._workspace_location = aml_client.workspaces.get(aml_client.workspace_name).location
        self._wait_time = 120  # seconds to wait between creating batches of workers
        self.servicebus_resource_group = (
            servicebus_resource_group.lower() if servicebus_resource_group else None
        )
        self.servicebus_namespace = servicebus_namespace.lower() if servicebus_namespace else None
        self.servicebus_topic = servicebus_topic.lower() if servicebus_topic else None
        self._create_servicebus_resources()
        self.cluster_type: str | None = None
        self.session = requests.Session()
        t = self._credential.get_token("https://management.azure.com/.default")
        self.tenant_id = jwt.decode(t.token, options={"verify_signature": False})["iss"].split("/")[
            3
        ]

    def _create_servicebus_resources(self):
        """
        Create a service bus topic and subscription for graceful shutdown if they do not exist already.
        """
        if (
            self.servicebus_topic is None
            or self.servicebus_namespace is None
            or self.servicebus_resource_group is None
        ):
            return
        check_if_topic_exists = f"az servicebus topic list  --namespace-name {self.servicebus_namespace} --resource-group {self.servicebus_resource_group}"
        check_if_subscription_exists = f"az servicebus topic subscription list  --namespace-name {self.servicebus_namespace} --resource-group {self.servicebus_resource_group} --topic {self.servicebus_topic}"

        create_topic = f"az servicebus topic create --resource-group  {self.servicebus_resource_group}  --namespace-name {self.servicebus_namespace} --name {self.servicebus_topic}"
        create_subscription = f"az servicebus topic subscription create --resource-group  {self.servicebus_resource_group}  --namespace-name {self.servicebus_namespace} --topic-name {self.servicebus_topic} --name shutdown"

        LOG.info(f"Creating service bus topic: {self.servicebus_topic} if it does not exist")
        d = azcli(check_if_topic_exists)

        topic_exists = False
        for topic in d:
            if topic["name"] == self.servicebus_topic:
                topic_exists = True
                break
        if topic_exists:
            LOG.debug("Found topic for graceful shutdown.")
        else:
            LOG.info("Creating topic for graceful shutdown.")
            d = azcli(create_topic)
        d = azcli(check_if_subscription_exists)

        subscription_exists = False
        for subscription in d:
            if subscription["name"] == "shutdown":
                subscription_exists = True
                break
        if subscription_exists:
            LOG.debug("Found subscription for graceful shutdown.")
        else:
            LOG.info("Creating subscription for graceful shutdown.")
            azcli(create_subscription)

    def get_current_state(self, extra_filters: list[dict] | None = None) -> State:
        """Get a lightweight state of the workforce (num_pending + num_running).

        For per-status breakdowns, use get_detailed_state() instead.

        The extra_filters field can be used to filter the jobs, the syntax is documented here:
        https://learn.microsoft.com/en-us/graph/filter-query-parameter?tabs=http
        """
        detailed = self.get_detailed_state(extra_filters=extra_filters)
        return self.State(
            num_pending=detailed.num_pending,
            num_running=detailed.num_running,
        )

    def get_detailed_state(self, extra_filters: list[dict] | None = None) -> DetailedState:
        """Get a detailed state of the workforce with per-status breakdowns.

        Returns a DetailedState with counts for each active job status
        (running, paused, queued, preparing, starting, waiting).
        No extra API cost over get_current_state() — uses the same list_jobs() call.

        The extra_filters field can be used to filter the jobs, the syntax is documented here:
        https://learn.microsoft.com/en-us/graph/filter-query-parameter?tabs=http
        """
        num_running = 0
        num_paused = 0
        num_queued = 0
        num_preparing = 0
        num_starting = 0
        num_waiting = 0

        for job in self.list_jobs(
            with_status=[
                "Paused",
                "Preparing",
                "Queued",
                "Running",
                "Starting",
                "Waiting",
            ],
            extra_filters=extra_filters,
        ):
            if job.status == "Running":
                num_running += 1
            elif job.status == "Paused":
                num_paused += 1
            elif job.status == "Queued":
                num_queued += 1
            elif job.status == "Preparing":
                num_preparing += 1
            elif job.status == "Starting":
                num_starting += 1
            elif job.status == "Waiting":
                num_waiting += 1
            else:
                raise RuntimeError(f"Unknown job status: {job.status}")

        return self.DetailedState(
            num_running=num_running,
            num_paused=num_paused,
            num_queued=num_queued,
            num_preparing=num_preparing,
            num_starting=num_starting,
            num_waiting=num_waiting,
        )

    def scale_to(self, num_workers: int, with_layoffs: bool = True) -> None:
        current_state = self.get_current_state()

        num_to_start = num_workers - current_state.num_running - current_state.num_pending

        ineq = "" if with_layoffs else "≥ "
        msg = f"Scaling to {ineq}{num_workers} workers. Currently have {current_state.num_running} running and {current_state.num_pending} pending."

        if num_to_start > 0:
            LOG.info(f"{msg} Requesting {num_to_start} new workers.")
            self.hire(num_to_start)
        elif num_to_start < 0 and with_layoffs:
            LOG.info(f"{msg} Stopping {-num_to_start} workers.")
            self.lay_off(-num_to_start)
        else:
            LOG.info(f"{msg} No change needed.")

    def list_jobs(
        self,
        with_status: Iterable[Status] | None = None,
        ordering: Literal["Asc", "Desc"] = "Asc",
        max_jobs: int | None = None,
        extra_filters: list[dict] | None = None,
    ) -> Generator[AmlJob, None, None]:
        """List jobs in the experiment, the extras field can be used to filter the jobs, the syntax is documented here: https://learn.microsoft.com/en-us/graph/filter-query-parameter?tabs=http. There is no official documentation with sample responses, as this endpoint is experimental and might change anytime."""
        if extra_filters is None:
            extra_filters = []
        if with_status is not None:
            extra_filters.append(
                {
                    "field": "annotations/status",
                    "operator": "eq",
                    "values": with_status,
                }
            )

        subscription_id = self._aml_client.subscription_id
        resource_group = self._aml_client.resource_group_name
        workspace = self._aml_client.workspace_name
        if workspace is None:
            raise RuntimeError("Workspace name is not set in the AzureML client.")

        url = f"https://ml.azure.com/api/{self._workspace_location}/index/v1.0/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.MachineLearningServices/workspaces/{workspace}/entities"
        payload = {
            "filters": [
                {"field": "type", "operator": "eq", "values": ["runs"]},
                {"field": "annotations/archived", "operator": "eq", "values": ["false"]},
                {
                    "field": "properties/experimentName",
                    "operator": "eq",
                    "values": [self._experiment_name],
                },
                *extra_filters,
            ],
            "order": [{"field": "properties/creationContext/createdTime", "direction": ordering}],
            "freeTextSearch": "",
            "pageSize": 100,  # 100 is the maximum page size on the AzureML website.
            "includeTotalResultCount": True,
            "searchBuilder": "AppendPrefix",
        }

        continuation_token: str | None = None  # Used for pagination.
        total_jobs_yielded = 0
        while True:
            token = self._credential.get_token("https://ml.azure.com/.default")
            headers = {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}
            response = self.session.post(
                url,
                json=dict(**payload, continuationToken=continuation_token),
                headers=headers,
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch jobs under experiment {self._experiment_name}: {response.text}"
                )

            response_data = response.json()

            if "value" not in response_data:
                raise RuntimeError(
                    f"Failed to fetch jobs under experiment {self._experiment_name}: {response_data}"
                )

            for raw_job in response_data["value"]:
                experiment = AmlExperiment(
                    id=raw_job["properties"]["experimentId"],
                    subscription_id=subscription_id,
                    resource_group=resource_group,
                    workspace=workspace,
                )

                yield AmlJob(
                    experiment=experiment,
                    status=raw_job["annotations"]["status"],
                    name=raw_job["annotations"]["displayName"],
                    start_time=(
                        _parse_utc(st)
                        if (st := raw_job["annotations"].get("effectiveStartTimeUtc")) is not None
                        else None
                    ),
                    cluster=raw_job["properties"]["compute"]["target"],
                    error_msg=(
                        err["message"] if (err := raw_job["annotations"].get("error")) else None
                    ),
                    metrics=(
                        {metric_name: data["lastValue"] for metric_name, data in metrics.items()}
                        if (metrics := raw_job["annotations"].get("metrics")) is not None
                        else {}
                    ),
                )
                total_jobs_yielded += 1

                if max_jobs is not None and total_jobs_yielded >= max_jobs:
                    return

            if (continuation_token := response_data.get("continuationToken")) is None:
                break

    def get_available_to_hire(self, current_state: State | None = None) -> int:
        """
        Determine how many new workers can be hired, based on the current state of the workforce
        and the cluster quota. Based on the maximum cluster size, the current number of nodes,
        and the number of already queued jobs, we determine how many new jobs can be started.
        Args:
            current_state (State, optional): The current state of the Workforce. Will be loaded
            when None. Can be used to speedup scheduling with lazy states.
        Returns:
            int: The maximum number of new workers that can be hired.
        """
        current_state = current_state or self.get_current_state()

        # for azureml, we check the cluster size, subtract the currently running (targetNodeCount)
        # then we subtract the already pending jobs
        cluster_info = self.get_compute_infos().properties.properties
        max_node_count = cluster_info.scaleSettings["maxNodeCount"]
        nb_available = max_node_count - cluster_info.targetNodeCount  # type: ignore[operator]
        max_to_hire = nb_available - current_state.num_pending
        return max_to_hire

    @staticmethod
    def _is_duplicate_job_name_error(exc: HttpResponseError) -> bool:
        """Return True if ``exc`` is the AzureML "job name already exists" error.

        The AzureML MFE returns a ``UserError`` whose inner error code is
        ``JobPropertyImmutable`` when a job with the same name has already
        been created. In the ``Workforce`` flow this can only be caused by
        the azure-core transport-retry policy re-sending a
        ``create_or_update`` whose first attempt already succeeded
        server-side (request names are 8-char random suffixes with ~10^12
        entropy, so a genuine collision is astronomically unlikely).
        """
        message = str(exc)
        if "JobPropertyImmutable" in message:
            return True
        # Fall back to parsing the structured error when azure-core does
        # not inline the inner code in ``str(exc)`` (older versions).
        err = getattr(exc, "error", None)
        code = getattr(err, "code", None)
        return code == "JobPropertyImmutable"

    def _submit_job(self, job: Command) -> None:
        """Submit ``job`` via ``MLClient.jobs.create_or_update``.

        Tolerates the SDK-internal retry race where a transport-level
        retry re-sends a request whose first attempt already succeeded,
        which surfaces as an ``HttpResponseError`` with inner code
        ``JobPropertyImmutable``. Because :meth:`_build_worker` and
        :meth:`hire` always generate a fresh 8-char random suffix per
        call, hitting that error on our *own* newly-minted job name is
        only possible via this retry race; we log at ``debug`` and treat
        it as success.
        """
        try:
            self._aml_client.jobs.create_or_update(job)
        except HttpResponseError as exc:
            if self._is_duplicate_job_name_error(exc):
                LOG.debug(
                    "SDK retry race on %s — job already created server-side, treating as success",
                    job.name,
                )
                return
            raise

    def _build_worker(self) -> Command:
        """Build a fresh worker job from the prototype, with a unique name.

        A shallow copy (``copy.copy``) is used rather than reusing the
        prototype object because :meth:`parallel_hire` submits many jobs
        concurrently; sharing a single ``Command`` instance across threads
        would race on ``.name`` / ``.experiment_name`` / ``.environment_variables``
        mutations. Deep-copying is not an option (see the comment in the
        previous implementation: ``InputsAttrDict`` is not deep-copyable),
        but shallow copy plus a fresh ``environment_variables`` dict is
        enough — no other attribute is mutated per-worker.
        """
        job = copy.copy(self._job)
        # Give the copy its own env dict so parallel workers don't race on setdefault.
        job.environment_variables = dict(job.environment_variables or {})
        acs = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
        if acs is not None:
            job.environment_variables.setdefault("APPLICATIONINSIGHTS_CONNECTION_STRING", acs)
        job.experiment_name = self._experiment_name
        random_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        job.name = f"{self._experiment_name}-{random_id}"
        return job

    def _run_parallel(
        self,
        fn: Callable[[Any], Any],
        items: list,
        *,
        workers: int,
        progress: bool,
        desc: str,
        item_label: Callable[[Any], str],
    ) -> tuple[int, int]:
        """Run ``fn(item)`` concurrently across ``items`` with a thread pool.

        Returns ``(succeeded, failed)``. Individual failures are logged as
        warnings and do not abort the batch — matching the semantics of the
        sequential :meth:`hire` / :meth:`lay_off` / :meth:`resume` variants.
        """
        if not items:
            return 0, 0
        succeeded = 0
        failed = 0
        pbar: Progress | None = (
            Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            if progress
            else None
        )
        pbar_cm = pbar if pbar is not None else nullcontext()
        with pbar_cm, ThreadPoolExecutor(max_workers=workers) as pool:
            task_id = pbar.add_task(desc, total=len(items)) if pbar is not None else None
            futures = {pool.submit(fn, item): item for item in items}
            for fut in as_completed(futures):
                item = futures[fut]
                try:
                    fut.result()
                    succeeded += 1
                except Exception as exc:
                    failed += 1
                    LOG.warning("%s failed for %s: %s", desc, item_label(item), exc)
                if pbar is not None and task_id is not None:
                    pbar.advance(task_id)
        return succeeded, failed

    def _progress_iter(
        self,
        items: Iterable[Any],
        *,
        desc: str,
        total: int,
        enabled: bool,
    ) -> Generator[Any, None, None]:
        """Yield from ``items``, advancing a Rich progress bar after each.

        The bar uses the same columns as :meth:`_run_parallel` so sequential
        and parallel variants look identical to the user. When
        ``enabled`` is False this is a transparent pass-through.
        """
        if not enabled:
            yield from items
            return
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as pbar:
            task_id = pbar.add_task(desc, total=total)
            for item in items:
                yield item
                pbar.advance(task_id)

    def hire(self, n: int, batch_size: int = 200, *, progress: bool = True) -> None:
        """Adds workers to the workforce.

        When ``progress`` is True (default) a Rich progress bar is shown,
        matching :meth:`parallel_hire`'s output.
        """
        for i in self._progress_iter(range(1, n + 1), desc="Hiring", total=n, enabled=progress):
            # not possible to deepcopy an azureml job, because class
            # azure.ai.ml.entities._job.pipeline._io.attr_dict.InputsAttrDict can not be copied
            job = self._job
            job.environment_variables = job.environment_variables or {}
            if acs := os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") is not None:
                job.environment_variables.setdefault("APPLICATIONINSIGHTS_CONNECTION_STRING", acs)
            job.experiment_name = self._experiment_name
            random_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
            job.name = f"{self._experiment_name}-{random_id}"
            LOG.debug(f"Creating worker {job.name}")
            self._submit_job(job)
            if i % batch_size == 0:
                # we do not want to overload the AzureML service/container registry with too many
                # requests, so we wait between every now and then
                time.sleep(self._wait_time)

    def parallel_hire(
        self,
        n: int,
        *,
        workers: int = 8,
        progress: bool = True,
    ) -> None:
        """Add ``n`` workers to the workforce concurrently using a thread pool.

        The first worker is submitted sequentially to prime any code /
        environment upload that ``MLClient.jobs.create_or_update`` performs
        on first use; once the uploaded artifact is cached in blob storage,
        the remaining ``n - 1`` submissions are issued in parallel.

        Individual submission failures are logged as warnings and do not
        abort the batch.

        Args:
            n: Number of workers to hire. Non-positive values are a no-op.
            workers: Size of the thread pool (default 8).
            progress: Show a Rich progress bar (default True).
        """
        if n <= 0:
            return
        # First hire primes any code/environment upload so subsequent parallel
        # submissions reuse the cached artifact in blob storage.
        self.hire(1, progress=False)
        remaining = n - 1
        if remaining == 0:
            return
        jobs = [self._build_worker() for _ in range(remaining)]
        _, failed = self._run_parallel(
            self._submit_job,
            jobs,
            workers=workers,
            progress=progress,
            desc="Hiring",
            item_label=lambda j: j.name,
        )
        if failed:
            LOG.warning("%d/%d hires failed", failed, remaining)

    def __str__(self):
        return f"Workforce(experiment_name={self._experiment_name})"

    # Sort key used to pick lay-off victims. Paused jobs come first because
    # they may have already hit Singularity's max-execution-time limit and
    # can no longer be resumed; cancelling them is the only way to free the
    # slot. Queued/Waiting follow (no work lost — never executed). Active
    # states (Preparing/Starting/Running) are last since they're burning
    # compute on user work.
    _LAYOFF_STATUS_PRIORITY: tuple[Status, ...] = (
        "Paused",
        "Queued",
        "Waiting",
        "Preparing",
        "Starting",
        "Running",
    )

    def _layoff_candidates(self, n: int) -> list[AmlJob]:
        """List active workers sorted by preferred-kill order, capped at ``n``."""
        candidates = list(self.list_jobs(with_status=list(self._LAYOFF_STATUS_PRIORITY)))
        if len(candidates) < n:
            LOG.warning(
                "Only %d workers to stop, but %d requested. Stopping all available workers.",
                len(candidates),
                n,
            )
            n = len(candidates)
        candidates.sort(
            key=lambda job: tuple(job.status == s for s in self._LAYOFF_STATUS_PRIORITY),
            reverse=True,
        )
        return candidates[:n]

    def _cancel_one(self, worker: AmlJob) -> None:
        """Cancel one worker, swallowing the AML permission-denied error as a warning."""
        LOG.debug("Stopping worker %s in state %s", worker.name, worker.status)
        try:
            self._aml_client.jobs.begin_cancel(worker.name)
        except HttpResponseError as e:
            if "Cannot execute Cancel because user does not have sufficient permission." in str(e):
                LOG.warning(
                    "Failed to stop worker %s: user does not have sufficient permission.",
                    worker.name,
                )
            else:
                raise e

    def lay_off(self, n: int, *, progress: bool = True) -> None:
        """Cancel ``n`` workers sequentially, preferring less-advanced jobs.

        When ``progress`` is True (default) a Rich progress bar is shown,
        matching :meth:`parallel_lay_off`'s output.
        """
        candidates = self._layoff_candidates(n)
        for worker in self._progress_iter(
            candidates, desc="Laying off", total=len(candidates), enabled=progress
        ):
            self._cancel_one(worker)

    def parallel_lay_off(
        self,
        n: int,
        *,
        workers: int = 8,
        progress: bool = True,
    ) -> None:
        """Cancel ``n`` workers concurrently using a thread pool.

        Selection order (less-advanced states first) is identical to
        :meth:`lay_off`; the cancellations themselves are dispatched
        concurrently.

        Args:
            n: Number of workers to cancel. Non-positive values are a no-op.
            workers: Size of the thread pool (default 8).
            progress: Show a Rich progress bar (default True).
        """
        if n <= 0:
            return
        candidates = self._layoff_candidates(n)
        _, failed = self._run_parallel(
            self._cancel_one,
            candidates,
            workers=workers,
            progress=progress,
            desc="Laying off",
            item_label=lambda w: w.name,
        )
        if failed:
            LOG.warning("%d/%d layoffs failed", failed, len(candidates))

    def _paused_candidates(self, n: int) -> list[AmlJob]:
        """List paused workers oldest-first, capped at ``n``.

        ``list_jobs`` uses ``ordering="Asc"`` (oldest first) by default,
        giving FIFO-fairness semantics for resume.
        """
        candidates = list(self.list_jobs(with_status=["Paused"]))
        if len(candidates) < n:
            LOG.warning(
                "Only %d paused workers, but %d requested. Resuming all available.",
                len(candidates),
                n,
            )
            n = len(candidates)
        return candidates[:n]

    # Max retry attempts for transient 5xx responses from the MFE resume API.
    # Under heavy load Singularity's CJP backend returns ``500`` wrapping a
    # ``503 DatabaseOverCapacity`` with a "Please retry in 1 second" hint.
    # Retrying in-process avoids forcing a full rerun of ``resume`` for every
    # transient blip.
    _RESUME_MAX_RETRIES = 3

    @staticmethod
    def _resume_retry_delay(response: requests.Response, attempt: int) -> float:
        """Return the sleep delay before the next resume retry.

        Honors ``x-ms-retry-after-ms`` (Azure) and ``Retry-After`` (standard)
        if present; otherwise uses exponential backoff (1s, 2s, 4s, ...)
        capped at 30s with a small jitter.
        """
        ms = response.headers.get("x-ms-retry-after-ms")
        if ms is not None:
            try:
                return max(0.0, float(ms) / 1000.0)
            except ValueError:
                pass
        ra = response.headers.get("Retry-After")
        if ra is not None:
            try:
                return max(0.0, float(ra))
            except ValueError:
                pass
        return min(2.0**attempt, 30.0) + random.uniform(0, 1)

    def _resume_one(self, worker: AmlJob) -> None:
        """Resume a single paused job via the MFE execution REST API.

        The AzureML ARM API (accessed via ``MLClient.jobs``) does not expose
        a resume method; the Machine Learning Front-End (MFE) execution
        endpoint does.

        If the job cannot be resumed because it exceeded Singularity's
        max job execution time, the job is cancelled instead — the slot is
        effectively dead and the only way to free it is to cancel.

        Other per-job 4xx responses (deleted, wrong state, missing
        permission, etc.) are logged as warnings and swallowed — they mean
        *this specific worker* can't be resumed but the batch should
        continue. Transient 5xx responses are retried up to
        :attr:`_RESUME_MAX_RETRIES` times honoring ``Retry-After``; if the
        server is still 5xx after retries, a :class:`RuntimeError` is
        raised so infra-level failures surface to the caller.
        """
        LOG.debug("Resuming worker %s", worker.name)
        subscription_id = self._aml_client.subscription_id
        resource_group = self._aml_client.resource_group_name
        workspace = self._aml_client.workspace_name
        if workspace is None:
            raise RuntimeError("Workspace name is not set in the AzureML client.")
        url = (
            f"https://ml.azure.com/api/{self._workspace_location}/execution/v1.0/"
            f"subscriptions/{subscription_id}/resourceGroups/{resource_group}/"
            f"providers/Microsoft.MachineLearningServices/workspaces/{workspace}/"
            f"experiments/{self._experiment_name}/runId/{worker.name}/resume"
        )
        token = self._credential.get_token("https://ml.azure.com/.default").token
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        for attempt in range(self._RESUME_MAX_RETRIES + 1):
            response = self.session.post(url, headers=headers, timeout=60)
            if response.status_code < 500 or attempt == self._RESUME_MAX_RETRIES:
                break
            delay = self._resume_retry_delay(response, attempt)
            LOG.warning(
                "Transient HTTP %d resuming worker %s; retrying in %.1fs (attempt %d/%d)",
                response.status_code,
                worker.name,
                delay,
                attempt + 1,
                self._RESUME_MAX_RETRIES,
            )
            time.sleep(delay)
        if 200 <= response.status_code < 300:
            return
        if 400 <= response.status_code < 500:
            body = response.text
            if "exceeded max job execution time" in body:
                LOG.warning(
                    "Worker %s cannot be resumed (exceeded max job execution time); "
                    "cancelling instead.",
                    worker.name,
                )
                self._cancel_one(worker)
                return
            LOG.warning(
                "Failed to resume worker %s: HTTP %d: %s",
                worker.name,
                response.status_code,
                body,
            )
            return
        raise RuntimeError(
            f"Failed to resume worker {worker.name}: HTTP {response.status_code}: {response.text}"
        )

    def resume(self, n: int, *, progress: bool = True) -> None:
        """Resume up to ``n`` paused workers sequentially (oldest-first).

        When ``progress`` is True (default) a Rich progress bar is shown,
        matching :meth:`parallel_resume`'s output.
        """
        candidates = self._paused_candidates(n)
        for worker in self._progress_iter(
            candidates, desc="Resuming", total=len(candidates), enabled=progress
        ):
            self._resume_one(worker)

    def parallel_resume(
        self,
        n: int,
        *,
        workers: int = 8,
        progress: bool = True,
    ) -> None:
        """Resume up to ``n`` paused workers concurrently using a thread pool.

        Jobs are selected oldest-first (FIFO); the resume calls themselves
        are dispatched concurrently. Individual failures are logged as
        warnings and do not abort the batch.

        Args:
            n: Number of workers to resume. Non-positive values are a no-op.
            workers: Size of the thread pool (default 8).
            progress: Show a Rich progress bar (default True).
        """
        if n <= 0:
            return
        candidates = self._paused_candidates(n)
        _, failed = self._run_parallel(
            self._resume_one,
            candidates,
            workers=workers,
            progress=progress,
            desc="Resuming",
            item_label=lambda w: w.name,
        )
        if failed:
            LOG.warning("%d/%d resumes failed", failed, len(candidates))

    def get_compute_infos(self) -> AmlComputeInfo:
        """for the used azureml compute cluster, get the available information, e.g. the number of unprovisioned nodes.
        The endpoint, including a sample response is documented here: https://learn.microsoft.com/en-us/rest/api/azureml/compute/get?view=rest-azureml-2024-10-01&tabs=HTTP#get-a-aml-compute"""
        compute_name = self._job.compute
        subscription_id = self._aml_client.subscription_id
        resource_group = self._aml_client.resource_group_name
        workspace = self._aml_client.workspace_name
        if workspace is None:
            raise RuntimeError("Workspace name is not set in the AzureML client.")
        url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.MachineLearningServices/workspaces/{workspace}/computes/{compute_name}?api-version=2021-04-01"

        token = self._credential.get_token("https://management.core.windows.net/.default")
        headers = {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}
        response = self.session.get(url, headers=headers)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch cluster information {compute_name}: {response.text}"
            )
        aml_info = AmlComputeInfo(**response.json())
        return aml_info
