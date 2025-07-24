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
