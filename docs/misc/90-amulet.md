# Launching workers with Amulet


In your amulet config, add

```yaml
jobs:
  job:
    command:
      - ai4s-jobq $JOBQ_STORAGE/$JOBQ_QUEUE worker --time-limit $JOBQ_TIME_LIMIT
```
As the `ai4s-job` storage and queue names are internally handled by environmental variables, the job command can be flexibly extended or, e.g., be used in a grid search. 
Then, use amulet as follows:

```shell
# running jobs:
ai4s-jobq mystorageaccount/queuename amlt --time-limit 24h -- run [amlt options] config.yml
# everything after -- is passed through to amlt, e.g.
ai4s-jobq mystorageaccount/queuename amlt -- --help
```
