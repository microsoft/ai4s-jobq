Dead Lettering
==============

If a task fails repeatedly, it will be moved to a dead letter queue after a configurable number of attempts.
This allows you to inspect and handle failed tasks separately, without blocking the processing of other tasks.

For the **storage queue** backend, the dead letter queue is a queue with the same
name as the tasks queue with the string "-failed" appended to it.

For the **Service Bus** backend, failed tasks are moved to the
`built-in dead-letter sub-queue <https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues>`_
of the main queue, which can be inspected via the Azure Portal or the Service Bus Explorer.

JobQ does not do anything with the tasks in the dead letter queue, it's up to you to inspect and handle them.
