# Dealing with Preemption

If your command supports checkpointing, you can try to trigger a checkpoint when your job is about to be preempted.
When using the `ShellCommandProcessor`, be sure to configure your shell to pass the signal to your python process.

```shell
# instead of:
python main.py

# run this:
set -o monitor
python main.py
```

Your process can catch the signal and eg trigger saving a checkpoint.

There's an important distinction here between Manifold and AzureML Compute:

- Manifold sends us SIGTERM and then stops the container after a timeout.

- On AzureML Compute, the azure VM announces a preemption, but then Azure may
  reconsider this decision and not preempt the VM after all. If this is too disruptive,
  and you don't have any checkpointing logic anyway, set
  `JOBQ_DISABLE_SCHEDULED_EVENTS=1`.

  The standard behavior is for jobq to send SIGTERM, wait for your command to
  finish, ignore its exit code, and reschedule the task.

If you're *not* using `ShellCommandProcessor`, you likely don't care that much about saving state when preempted.
One thing you can consider is that the `ProcessPool` installs a signal handler that passes the signal to all subprocesses.
Your subprocesses can then install their own signal handlers as they please.


## Preemption walkthrough

If you want your code to be preemptible, you can start with the following sample script which is called dummy_task.py:
```python
import signal
import time


def handle_event(
    event: signal.Signals,
    stack_frame,
):
    """Handle preemption signals."""
    print(f"CHECKPOINTING Received signal: {event}")


def main():
    print("Starting dummy task, will sleep 10 seconds.")
    signal.signal(signal.SIGTERM, handle_event)

    # Simulate a long-running task
    time.sleep(10)
    print("Finished dummy task.")


if __name__ == "__main__":
    main()
```

You can then queue this task:

```python
from ai4s.jobq import JobQ, batch_enqueue
from azure.identity import AzureDefaultCredential

your_queue_name = "demo-preemption"
your_storage_account = "<YOUR-STORAGE-ACCOUNT>"
nb_tasks_to_queue = 40


all_commands_to_run = [
    "set -o monitor && python ./dummy_task.py"
] * nb_tasks_to_queue

# Enqueue the commands to the JobQ queue.
async with JobQ.from_storage_queue(
    your_queue_name, storage_account=your_storage_account, credential=AzureDefaultCredential()
) as queue:
    await batch_enqueue(
        queue,
        all_commands_to_run,
    )
```

You can then start a local ai4s-jobq worker to process the tasks:
Note that this example uses `--num-workers 2` to start two workers on the same node, so two tasks run in parallel.
```shell
ai4s-jobq <YOUR-STORAGE-ACCOUNT>/demo-preemption worker --num-workers 2 --heartbeat --max-consecutive-failures 5 --time-limit 1d
```

To simulate a preemption, press Ctrl-C in the terminal where the worker is running. Look at the output, you can see that the CHECKPOINTING message is printed when the SIGTERM signal is received.

**Best practices**: Your sigterm-handles should be quick, and should not raise exceptions. It should not call sys.exit() or similar. It should ideally just signal to the main thread to checkpoint and exit.
