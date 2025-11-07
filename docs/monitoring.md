# Monitoring

To get some insight into the status of your JobQ queues and workers, the workers can transmit telemetry data (logs) to a centralized 'Azure Application Insights' instance. That information can then be used to make, for example, Grafana dashboards.

![Grafana dashboard example](_static/jobq-telemetry.png)

Steps:

1. Create a [Log Analytics workspace](https://ms.portal.azure.com/?feature.msaljs=true#create/Microsoft.LogAnalyticsOMS) in the project's resource group if not existing already. Use the naming convention `{project}-la`. DO NOT re-use the Log Analytics workspace associated to an Azure ML workspace, this will complicate things later on.

2. Create an [Application Insights instance](https://ms.portal.azure.com/?feature.msaljs=true#create/Microsoft.AppInsights) in the project's resource group if not existing already. Make sure to reference the Log Analytics workspace from step 1. Use the naming convention `{project}-appinsights`. DO NOT re-use the Application Insights instance associated to an Azure ML workspace, this will complicate things later on.

3. Supply two environment variables to your worker to connect it to App Insights.
    - `APPLICATIONINSIGHTS_CONNECTION_STRING`. You can get this from your App Insights instance in the "Overview" tab.
    - `JOBQ_ENVIRONMENT_NAME`. This is just an arbitrary identifier of where the worker runs. Could include the instance type, region, or cluster name. Will be used for grouping in the dashboard.

## Viewing the data

### Plain App Insights queries

You can query the logs in App Insights or the connected Log Analytics workspace directly. Try a query like:

```kusto
    traces
    | where message startswith "Task starting"
    | limit 100
```

   In Log Analytics (the service that App Insights uses under the hood), the names of the collections are slightly different, for example `AppTraces` instead of `Traces`.

### JobQ track

You can run ``ai4s-jobq {queue spec} track {workspace_id}``. This will open a local http server that runs
common queries repeatedly and visualizes the results as graphs in your browser.
Note that the ``workspace_id`` is *not* the resoure ID but the UUID of the workspace that you can
find in the "Overview" tab of your Log Analytics workspace instance on the azure portal.
You can also set this via the environment variable ``LOG_ANALYTICS_WORKSPACE_ID`` and omit it on the command line.


### Grafana dashboard

1. Create a [Grafana instance](https://learn.microsoft.com/en-us/azure/managed-grafana/quickstart-managed-grafana-portal) if not existing already. This is typically shared across projects. 

2. [Import the pre-made dashboard](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/) into Grafana if not existing already. Make sure to [assign the "Monitoring Reader" role](https://learn.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal) for your [Grafana system identity](https://learn.microsoft.com/en-us/azure/managed-grafana/how-to-authentication-permissions#use-a-system-assigned-managed-identity) to your project resource groups (or whole subscription) so that the dashboard can read the JobQ logs.

3. Navigate to the JobQ dashboard, select the correct Subscription and Log Analytics workspace from the dropdowns, and make sure everything works, meaning no errors are shown and some data points are visible while workers are running.
