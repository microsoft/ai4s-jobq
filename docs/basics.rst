Basic Operation
===============

Choosing a backend
------------------

``ai4s.jobq`` supports two Azure queue backends:

- **Azure Storage Queue** — simple and cheap. Good for most workloads. Requires a storage account.
- **Azure Service Bus** — offers built-in `dead-letter queues <https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues>`_
  and non-destructive message peeking, making it easier to inspect in-flight
  and failed tasks. Recommended when observability into queue state matters.

Both backends are included in the base install — no extras required.


Queue specification
-------------------

A queue is specified by a storage account and a queue name:

.. prompt:: bash $ auto

   # for Azure Storage Queue
   export JOBQ="mystorageaccount/test-queue"

   # for Azure Service Bus Queue
   export JOBQ="sb://servicebusname/testqueue"

You should pick a name that avoids conflicts with other people. All the tasks on a queue are expected to run in the same "environment" and on the same "hardware". If you want to send some tasks to GPU workers and other tasks to CPU workers, use separate queue names.

Here, tasks are represented as *bash* commands.


.. prompt:: bash $ auto

   # send a single task
   $ ai4s-jobq $JOBQ push -c "echo hello"

   # execute a single task
   $ ai4s-jobq $JOBQ pull

   # pull and execute tasks in a loop
   $ ai4s-jobq $JOBQ worker

Pull the first-pushed job from the queue and execute it.
- If there are no jobs left, ``ai4s-jobq`` exits with code 0. If the worker is running on Azure ML, the job would succeed.
- If the job fails, it is put back in the queue, until the max number of retries is exceeded.

If a worker dies while processing a task, the task will reappear in the queue
after ``--visibility-timeout``. For Service Bus queues, tasks reappear
after the lock expires (this is a queue-level setting, queues created by
ai4s-jobq have a 5 minutes lock duration).

If the message lock is lost while a task is still running (e.g. due to a
network interruption that prevents lock renewal), the worker automatically
cancels the in-progress task and moves on without settling the message.
The message becomes available for another worker to pick up, avoiding
duplicate processing.

When pushing commands, you can also:
- specify environment variables, eg. ``-e AMLT_OUTPUT_DIR=/mnt/default/some/dir``
- enable background directory syncing like in amulet ``--bg-dirsync-to /mnt/default/some/dir``, and let your job write to ``$AMLT_DIRSYNC_DIR``.


**Queueing many jobs from CLI**

When running ``ai4s-jobq push`` without the ``-c`` option, it will read commands from
standard input until EOF.
Each line corresponds to one task. You can either specify shell commands, or
kwargs as json. kwargs should start with an opening brace. For example, the
following two tasks do the same thing:

.. prompt:: bash $ auto

   $ cat tasks.txt
   {"cmd": "echo Hello $NAME", env={"NAME": "John Doe"}}
   echo Hello John Doe

   # enqueue both of them
   $ cat tasks.txt | ai4s-jobq $JOBQ push
