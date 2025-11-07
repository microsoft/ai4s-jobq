# Graceful shutdown

Ideally, you have already walked through the {doc}`workforce documentation <30-workforce>` and have a workforce running. If not, we recommend to do that first.


## Use Case and Implementation Details

This feature is used to enable laying off the workers without losing progress in currently running tasks. Instead of canceling the jobs, we communicate to workers directly to shut down after finishing their current task. If the workers support checkpointing and restarting, this is particularly useful.

A back channel communication is established via [servicebus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/) where users can publish events that can be picked up by the workers.

When a worker process started, a PID file is created for that process. While running, the worker process listens for event messages on a given servicebus topic.

### Shutdown modes

There are two different shutdown events: `graceful-downscale` and `do-not-accept-new-tasks`.
When a `graceful-downscale` event is received from the `shutdown` subscription, the PID files are checked to identify process IDs of all workers on the same node.
These processes are canceled using a `SIGTERM` signal. Worker processes that are not configured to listen the same topic will not receive the signal.
When a worker process receives a `SIGTERM` signal, it completes the current task and shuts down.
In case of a `do-not-accept-new-tasks` event, the worker process will not be terminated, but will stop picking up new tasks from the queue.

Use the same topic for workers that you may want to scale up/down together. A common scenario is to assign the same topic to the workers within a region.

## Prerequisites

If you want to use the graceful downscaling feature (see below), you will first need to have a servicebus with the required permissions configured. This needs to be created once per team and we have a terraform script for this. Please reach out to the infrastructure team to get started.

## Automatically create service bus topic and subscription for graceful downscaling

Each workforce will automatically create a service bus topic and subscription if they do not exist already. You will need to specify the service bus resource group and namespace. The topic name can be anything, for example the experiment name. The subscription name is fixed to "shutdown" as this is what the workers listen to. A sample command can then look like this:

```python
workforce = Workforce(
    exp_name, task,
    credential=credential,
    aml_client=aml_client,
    servicebus_resource_group="electronic-structure-rg",
    servicebus_namespace="livdft",
    servicebus_topic=exp_name,
    )
```


### Set Environment Variables

The following environment variables need to be set in your amulet file if you are using amulet to schedule:

- `WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE`: Azure service bus namespace including the host name.
- `WORKFORCE_CONTROL_TOPIC_NAME`: Topic to publish shutdown messages.
  - A subscription called `shutdown` needs to be created in this topic, this is usually done by the workforce automatically the first time it is run.


## Trigger a Graceful Shutdown

To perform a graceful shutdown, you need to send graceful-shutdown messages to the `WORKFORCE_CONTROL_TOPIC_NAME`.

Below script sends a message to trigger a graceful downscale in one node. The first worker that picks up the message will terminate after it completes the currently running task.

Topic name configuration can be used to be more specific on which nodes to target, eg. setting same topic name for the nodes in same cluster or region.

Here is an example code snippet to send a graceful-downscale message.

```python
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.identity import AzureCliCredential
import json

credential = AzureCliCredential()

WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE = ""
WORKFORCE_CONTROL_TOPIC_NAME = ""
shutdown_type = "graceful-downscale"  # or "do-not-accept-new-tasks"
nb_nodes_to_shutdown = 1

with ServiceBusClient(WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE, credential) as client:
    with client.get_topic_sender(WORKFORCE_CONTROL_TOPIC_NAME) as sender:
        body = json.dumps(
            {"operation": shutdown_type}
        )
        for _ in range(nb_nodes_to_shutdown):
            sender.send_messages(ServiceBusMessage(body))
```
