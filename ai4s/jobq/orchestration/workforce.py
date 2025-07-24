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

import logging
import random
import string
import time
from collections.abc import Generator, Iterable
from datetime import datetime
from typing import Literal

import requests
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Command
from azure.core.credentials import TokenCredential
from dateutil.parser import parse as _parse_utc
from pydantic import BaseModel, ConfigDict

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
        num_queued: int
        num_running: int

    def __init__(
        self,
        experiment_name: str,
        worker_prototype: Command,
        *,
        aml_client: MLClient,
        credential: TokenCredential,
    ):
        self._job = worker_prototype
        self._experiment_name = experiment_name
        self._aml_client = aml_client
        self._credential = credential
        self._workspace_location = aml_client.workspaces.get(aml_client.workspace_name).location
        self._wait_time = 120  # seconds to wait between creating batches of workers

    def get_current_state(self, extra_filters: list[dict] | None = None) -> State:
        """List jobs in the experiment, the extras field can be used to filter the jobs, the syntax is documented here: https://learn.microsoft.com/en-us/graph/filter-query-parameter?tabs=http"""
        num_queued = 0
        num_running = 0

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
            elif job.status in ("Queued", "Preparing", "Paused", "Starting", "Waiting"):
                num_queued += 1
            else:
                raise RuntimeError(f"Unknown job status: {job.status}")

        return self.State(
            num_queued=num_queued,
            num_running=num_running,
        )

    def scale_to(self, num_workers: int, with_layoffs: bool = True) -> None:
        current_state = self.get_current_state()

        num_to_start = num_workers - current_state.num_running - current_state.num_queued

        ineq = "" if with_layoffs else "≥ "
        msg = f"Scaling to {ineq}{num_workers} workers. Currently have {current_state.num_running} running and {current_state.num_queued} queued."

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

        session = requests.Session()
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
            response = session.post(
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

    def hire(self, n: int, batch_size=200) -> None:
        """Adds workers to the workforce."""
        for i in range(1, n + 1):
            job = self._job
            job.experiment_name = self._experiment_name
            random_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
            job.name = f"{self._experiment_name}-{random_id}"
            LOG.debug(f"Creating worker {job.name}")
            self._aml_client.jobs.create_or_update(job)
            if i % batch_size == 0:
                # we do not want to overload the AzureML service/container registry with too many
                # requests, so we wait between every now and then
                time.sleep(self._wait_time)

    def __str__(self):
        return f"Workforce(experiment_name={self._experiment_name})"

    def lay_off(self, n: int) -> None:
        """Removes workers from the workforce."""
        candidates_to_lay_off = list(
            self.list_jobs(
                with_status=[
                    "Paused",
                    "Preparing",
                    "Queued",
                    "Running",
                    "Starting",
                    "Waiting",
                ]
            )
        )
        if len(candidates_to_lay_off) < n:
            LOG.warning(
                f"Only {len(candidates_to_lay_off)} workers to stop, but {n} requested. Stopping all available workers."
            )
            n = len(candidates_to_lay_off)

        # Sort the jobs by their state.
        # We prefer to kill jobs that are in less advanced states.
        candidates_to_lay_off.sort(
            key=lambda job: (
                job.status == "Queued",
                job.status == "Waiting",
                job.status == "Paused",
                job.status == "Preparing",
                job.status == "Starting",
                job.status == "Running",
            ),
            reverse=True,
        )

        for worker in candidates_to_lay_off[:n]:
            LOG.debug(f"Stopping worker {worker.name} in state {worker.status}")
            self._aml_client.jobs.begin_cancel(worker.name)

    def get_compute_infos(self) -> AmlComputeInfo:
        """for the used compute cluster, get the available information, e.g. the number of unprovisioned nodes.
        The endpoint, including a sample response is documented here: https://learn.microsoft.com/en-us/rest/api/azureml/compute/get?view=rest-azureml-2024-10-01&tabs=HTTP#get-a-aml-compute"""
        compute_name = self._job.compute
        subscription_id = self._aml_client.subscription_id
        resource_group = self._aml_client.resource_group_name
        workspace = self._aml_client.workspace_name
        if workspace is None:
            raise RuntimeError("Workspace name is not set in the AzureML client.")
        session = requests.Session()
        url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.MachineLearningServices/workspaces/{workspace}/computes/{compute_name}?api-version=2021-04-01"

        token = self._credential.get_token("https://management.core.windows.net/.default")
        headers = {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}
        response = session.get(url, headers=headers)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch cluster information {compute_name}: {response.text}"
            )
        aml_info = AmlComputeInfo(**response.json())
        return aml_info
