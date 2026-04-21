# Deduplication & Deterministic Task IDs

## Overview

By default, `ai4s-jobq` generates **deterministic task IDs**—identical tasks
always produce the same ID (the MD5 hash of the serialized task content).
When combined with Azure Service Bus **duplicate detection**, re-submitting the
same task within the detection window is silently ignored by the broker.

This is useful for fault-tolerant pipelines where a producer might retry
enqueueing after a transient failure without knowing whether the first attempt
succeeded.

## How it works

1. When a task is pushed, its ID is computed as `md5(serialized_payload)`.
2. The task ID is sent as the Service Bus `MessageId`.
3. If the queue has duplicate detection enabled, Service Bus drops any message
   whose `MessageId` was already seen within the detection window (7 days by
   default for queues created by `ai4s-jobq`).

Queues created by `ai4s-jobq` v3.0+ have duplicate detection enabled
automatically. **Existing queues** created by earlier versions must be deleted
and recreated to gain this capability—Azure does not allow enabling duplicate
detection on an existing queue.

## Configuration

| Parameter | CLI flag | Default | Description |
|---|---|---|---|
| `JOBQ_DETERMINISTIC_IDS` | n/a | `true` | When `true`, task IDs are the MD5 hash of the payload. When `false`, a random UUID is generated for each task. |
| `duplicate_detection_window` | `--dedup-window` | `7` (days) | Duration of the Service Bus duplicate detection history window. Passed as integer days on the CLI or as a `timedelta` in the Python API. |

Set `JOBQ_DETERMINISTIC_IDS=false` if you intentionally want every push to
create a distinct message, even for identical payloads.

## Mismatch warning

On startup, `ai4s-jobq` checks whether the queue's duplicate-detection setting
is consistent with `JOBQ_DETERMINISTIC_IDS` and logs a warning if not:

- **Deterministic IDs enabled, but queue has no duplicate detection**—IDs are
  deterministic but the broker won't actually deduplicate. Delete and recreate
  the queue.
- **Queue has duplicate detection, but deterministic IDs are disabled**—random
  UUIDs mean the broker will never see a duplicate. Either enable deterministic
  IDs or recreate the queue without duplicate detection.

## Azure Storage Queue

Azure Storage Queues do not support duplicate detection at the broker level.
Deterministic IDs are still generated (and can be useful for tracking), but
deduplication must be handled application-side if needed.

## Retry behavior on Service Bus

Azure Service Bus does not support in-place message updates. When a task
fails and would normally be "replaced" with a decremented retry counter
(as the Storage Queue backend does), the Service Bus backend instead
**does nothing**—the message is re-delivered with its original content
after the lock expires.

This means `num_retries` is **not decremented** on Service Bus. If you
need retry budgets, track attempts in your own application code or rely
on the Service Bus `MaxDeliveryCount` setting (queues created by
`ai4s-jobq` use a high delivery count of 1000 to avoid interfering with
application-level retry logic).
