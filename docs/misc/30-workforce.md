#  Workforce Management

JobQ comes with tools for managing groups of workers executing the same task.
We call such a group a "workforce".


## Workforce Setup

A workforce starts a number of azureml jobs in an azureml workspace. Therefore, you need to have a workspace set up. This workspace (`WORKSPACE_NAME`) is in one azure subscription (`SUBSCRIPTION_ID`) and resource group (`RESOURCE_GROUP_NAME`). All jobs are started in one experiment. This is identified by the experiment name (`exp_name`).


The first step is to define a task that can be executed by the workforce. You will need an azure.ai.ml.command task. This can be either an azureml or a singularity task. You will need to specify the identity (`IDENTITY_RESOURCE_ID`) which should execute the task. You will also need to specify a docker image (`DOCKER_IMAGE`) that contains the code to be executed. The task can be configured with environment variables. The task can then be submitted to the workforce. The compute cluster will already need to exist in the azureml workspace. 

```python
from azure.ai.ml import command
from azure.ai.ml.entities import Environment, ManagedIdentityConfiguration

# example environment variables for identity and monitoring
environment_variables = {
    "APPLICATIONINSIGHTS_CONNECTION_STRING": APPLICATIONINSIGHTS_CONNECTION_STRING,
    "_AZUREML_SINGULARITY_JOB_UAI": IDENTITY_RESOURCE_ID,
    "AZURE_CLIENT_ID": IDENTITY_CLIENT_ID,
}

task = command(
        command=f"ai4s-jobq {storage_account}/{queue_name} worker --num-workers {NB_WORKERS} --heartbeat --max-consecutive-failures 5 --time-limit 14d --emulate-tty",
        compute="es-e32adsv5-uksouth",
        identity=ManagedIdentityConfiguration(resource_id=IDENTITY_RESOURCE_ID),
        environment_variables=environment_variables,
        environment=Environment(image=DOCKER_IMAGE), # it is recommended to use non-anonymous environments, but this is skipped for this example
        shm_size="256MB",
        tags={"ProjectID": PROJECT_ID},
        timeout=14 * 24 * 3600, # 14 days
    )
```

To connect the workforce to your workspace and task:
```python
from ai4s.jobq.orchestration.workforce import Workforce
from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
credential = DefaultAzureCredential()
aml_client = MLClient(
        credential=credential,
        subscription_id=SUBSCRIPTION_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        workspace_name=WORKSPACE_NAME,
    )
exp_name = "my-first-workforce"
workforce = Workforce(exp_name, task, credential=credential, aml_client=aml_client)
```

To then start 3 jobs which execute the task:
```python
workforce.hire(3)
```

To layoff workers:
```python
workforce.lay_off(2)
```

To scale to a specific number of workers (this will hire/layoff workers as needed):
```python
workforce.scale_to(1, with_layoffs=True)
```


## Multiregion workforce

If you want to run a workforce across multiple regions, you can use the `MultiRegionWorkforce` class. This class allows you to specify multiple workforces in different regions and manage workers across them. It is important to note that the task should be the same, but the experiment name should be different, e.g. include the name of the region.

```python
from ai4s.jobq.orchestration.workforce import MultiRegionWorkforce

# Define your workforces for each region
workforce_us = Workforce("my-first-workforce-eastus", task, credential=credential, aml_client=aml_client)
workforce_eu = Workforce("my-first-workforce-northeurope", task, credential=credential, aml_client=aml_client)

# Create a multi-region workforce which scales based on the size of the queue
multi_region_workforce = MultiRegionWorkforce(
    storage_account=storage_account,
    queue_name=queue_name,
    workforces=[workforce_us, workforce_eu]
    num_workers=NB_WORKERS,  # number of workers per job
    )
```

The multiregion workforce has a feature to automatically determine the number of recommended workers based on the size of the queue. You can see the implementation [here](../../ai4s/jobq/orchestration/workforce.py#L87). Right now, it gets the size of the queue, divides it by the amount of workers per job, then checks how many jobs are already running or queued over all workforces and has a simple if/else logic to determine the number of workers. Also, it only hires at most 10 times the _number of workers currently running + 1_ to avoid scaling up too quickly. If you want your own custom logic, you can subclass the `MultiRegionWorkforce` and override the `determine_number_of_workers` method.

So for example, if you queue size is 1600 and we have 4 workers per job, it would want to scale up to 50 workers. Depending on how many are already running:
* if nothing runs, then `determine_number_of_workers` will return 10
* if 3 workers are already running (e.g. because it hired 10 but 7 are still queued), then it will 40


```python
recommended_workers = multi_region_workforce.determine_number_of_workers()
```

If you then run the `MultiregionWorkforce`, it will automatically scale the workers across the regions based on the recommended number of workers, similarly to the `Workforce.scale_to` command above. The workforce will determine the number of workers needed, check in which region workers are available and then scale the workers accordingly.

```python
multi_region_workforce.run()
```
## Access to data
If filesystem access to data is required (read/write), blob storage can be *mounted* using the following tweak to setting up the `command`:

```python
from azure.ai.ml import Output
from azure.ai.ml.constants import InputOutputModes

# container and storage account names can be read from a blob URL, e.g.,
# https://STORAGE_ACCOUNT_NAME.blob.core.windows.net/CONTAINER_NAME/
output = Output(
    path=f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    mode=InputOutputModes.RW_MOUNT,
)

# set up the command as above, adding `output`:
task = command(
        command=...,
        outputs={"blobstor": output},
        ...
)
```

The directory will be mounted to a path accessible via `${{outputs.blobstor}}`.
To link it to a specified cache directory `cache_dir` on the local compute, the following string can be prepended to the command string:

```python
f'mkdir -p {cache_dir} && ln -s "${{outputs.blobstor}}" {os.path.join(cache_dir, CONTAINER_NAME)} && '
```


## Back Channel Communication

### Graceful Downscaling

#### Use Case and Implementation Details

This feature is used to enable laying off the workers without losing progress in currently running tasks. Instead of canceling the jobs, we communicate to workers directly to shut down after finishing their current task.

A back channel communication is established via [servicebus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/) where users can publish events that can be picked up by the workers.

When a worker process started, a PID file is created for that process. While running, the worker process listens for event messages on a given servicebus topic.
When a `graceful-downscale` event is received from the `shutdown` subscription, the PID files are checked to identify process IDs of all workers on the same node.
These processes are canceled using a `SIGINT` signal. Worker processes that are not configured to listen the same topic will not receive the signal.
When a worker process receives a `SIGINT` signal, it completes the current task and shuts down.

Use the same topic for workers that you may want to scale up/down together. A common scenario is to assign the same topic to the workers within a region.

#### Setup

##### Install JobQ with `workforce` distribution
```bash
git clone https://github.com/msr-ai4science/ai4s-jobq
cd ai4s-jobq
pip install [-e] '.[workforce]'
```

##### Create Servicebus, Topic, and Subscription

Follow azure instructions to determine / create a servicebus, at least one topic, and a subscription named "shutdown" in the topic.

##### Set Environment Variables

The following environment variables need to be set:

- `WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE`: Azure service bus namespace including the host name.
- `WORKFORCE_CONTROL_TOPIC_NAME`: Topic to publish shutdown messages.
  - A subscription called `shutdown` needs to be created in this topic.

#### Trigger a Graceful Shutdown

To perform a graceful shutdown, you need to send graceful-shutdown massages to the `WORKFORCE_CONTROL_TOPIC_NAME`.

Below script sends a message to trigger a graceful downscale in one node. The first worker that picks up the message will terminate after it completes the currently running task.

Topic name configuration can be used to be more specific on which nodes to target, eg. setting same topic name for the nodes in same cluster or region.

Here is an example code snippet to send a graceful-downscale message.

```py
from azure.servicebus import ServiceBusClient, ServiceBusMessage 
from azure.identity import AzureCliCredential
import json

WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE = ""
WORKFORCE_CONTROL_TOPIC_NAME = "" 

credential = AzureCliCredential()

with ServiceBusClient(WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE, credential) as client:
    with client.get_topic_sender(WORKFORCE_CONTROL_TOPIC_NAME) as sender:
        body = json.dumps(
            {"operation": "graceful-downscale"}
        )
        sender.send_messages(ServiceBusMessage(body))
```
