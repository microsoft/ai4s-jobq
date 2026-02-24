---
name: ai4s-jobq-cli
description: >-
  Authoritative reference for the ai4s-jobq CLI for managing distributed job queues on Azure Storage Queues and Azure Service Bus.
---

  When constructing ai4s-jobq commands, you MUST look up the exact option names
  from this document. Do NOT guess or infer option names from other CLIs.
  Every option for every command is listed below.

# ai4s-jobq CLI Reference

This skill provides a complete reference of all `ai4s-jobq` CLI commands,
their options, and arguments. Use it to help users construct correct
`ai4s-jobq` command invocations.

`ai4s-jobq` is a distributed job queue built on Azure Storage Queues and Azure Service Bus. Users push tasks; workers pull and process them asynchronously.

## Queue specification

All commands require a `SERVICE/QUEUE_NAME` argument (or the `JOBQ_STORAGE_QUEUE` environment variable):

```
# Azure Storage Queue
ai4s-jobq mystorageaccount/my-queue <command>

# Azure Service Bus
ai4s-jobq sb://myservicebus/my-queue <command>
```

## Tips for agents

**Only use options and arguments documented below.** Do not invent or guess option names. Every available option for each command is listed explicitly.

## `ai4s-jobq amlt`

Launch Amulet with JOBQ_STORAGE, JOBQ_QUEUE, and JOBQ_TIME_LIMIT set, so they can be referenced in the yaml file.

    Usage:

    
      $ ai4s-jobq storage0account/queue0name amlt run my-config.yaml

    So in the easiest case, you could do

    
      $ cat commands.txt | ai4s-jobq storage0account/queue0name push
      $ ai4s-jobq storage0account/queue0name amlt -- run config.yaml

    To ensure that jobq can shut down your task cleanly in the case of preemption,
    in the config.yaml, ensure that you launch ai4s-jobq like so:

    
      command:
        - ai4s-jobq ... & trap "kill -15 $!" TERM ; wait $!

**Parameters:**

  - `--time-limit`, `-t`: Soft time limit. Tasks will receive SIGTERM when this is reached. (default: `24h`)
  - `AMLT_ARGS` (multiple)

## `ai4s-jobq clear`

Remove all jobs from the queue.

## `ai4s-jobq peek`

Peek at the next job in the queue.

**Parameters:**

  - `-n` `INTEGER`: how many messages to show max. For storage queue, <=32 is supported. For service bus, use -1 for 'all messages' (default: `1`)
  - `--json`

**Examples:**

```
ai4s-jobq $JOBQ peek -n 5 --json  # peek at 5 messages as JSON
```

## `ai4s-jobq pull`

Pull and execute a job

**Parameters:**

  - `--visibility-timeout`: Visibility timeout. (default: `10m`)
  - `--proc`: Processor class to use. (default: `shell`)

## `ai4s-jobq push`

Enqueue a new job to the job queue.

**Parameters:**

  - `--num-retries` `INTEGER`: Number of retries. (default: `0`)
  - `--num-workers` `INTEGER`: Number of enqueue-workers. (default: `10`)
  - `--command`, `-c`: The command(s) to execute.
  - `--wait`: Wait for the job to finish and print its return value.
  - `--bg-dirsync-to`: Synchronize the $AMLT_DIRSYNC_DIR to this location in the background.
  - `--env`, `-e` `ENV-VAR`: Environment variables to set.
  - `--dedup-window`: Duplicate detection window, e.g. 7d, 12h (Service Bus only, default: 7d).

**Examples:**

```
ai4s-jobq $JOBQ push -c 'echo hello'  # push a single command
cat tasks.txt | ai4s-jobq $JOBQ push   # push commands from stdin
```

## `ai4s-jobq sas`

Get a SAS token for the queue.

**Parameters:**

  - `--expiry`, `-e`: Time until SAS token expires. (default: `24h`)

## `ai4s-jobq size`

Get the approximate size of the queue.

## `ai4s-jobq track`

Track the queue size and print it every 10 seconds.

    You can set JOBQ_LA_WORKSPACE_ID to the workspace ID of your Azure Log Analytics workspace.

**Parameters:**

  - `LOG_ANALYTICS_WORKSPACE` **(required)**
  - `-p` `INTEGER`: Port to run the dashboard on. (default: `8050`)
  - `--subscription-id`: Azure Subscription ID to use. This is only needed when obtaining the workspace ID from an instrumentation key.
  - `--debug`: Enable debug logging.

## `ai4s-jobq worker`

Like pull, but start multiple async workers.

**Parameters:**

  - `--visibility-timeout`: Visibility timeout. (default: `10m`)
  - `--num-workers`, `-n` `INTEGER`: Number of workers. (default: `1`)
  - `--max-consecutive-failures` `INTEGER`: Maximum number of consecutive failures before exiting. (default: `2`)
  - `--time-limit`: Soft time limit. Tasks will receive SIGTERM when this is reached. (default: `1d`)
  - `--heartbeat`, `--no-heartbeat`: Enable heartbeat for long running tasks, extending visibility_timeout indefinitely. Defaults to enabled.
  - `--proc`: Processor class to use. (default: `shell`)
  - `--emulate-tty`, `-t`: Emulate a TTY for the worker, forces line buffering (useful if logs get lost when preempted).

**Examples:**

```
ai4s-jobq $JOBQ worker -n 4 --time-limit 12h  # 4 parallel workers, 12h limit
```

## Documentation References

Detailed documentation is available in the `references/` directory.
Load these on demand for in-depth information on specific topics.

### Getting Started

- [Choosing a backend, queue specification, pushing and pulling jobs](references/basics.md)
- [Python API for queueing and running tasks programmatically](references/api.md)

### Operations

- [Monitoring with Azure Application Insights and Grafana](references/monitoring.md)
- [Managing worker fleets with Azure ML](references/workforce.md)
- [Graceful shutdown and preemption handling](references/graceful-shutdown.md)
- [Dealing with preempted workers](references/preemption.md)

### Advanced Topics

- [Dead-letter queues for failed messages](references/dead-lettering.md)
- [Handling large payloads via blob storage](references/large-objects.md)
- [Duplicate detection with Service Bus](references/deduplication.md)
