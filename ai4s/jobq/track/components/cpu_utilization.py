# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.express as px
from dash import Input, Output
from dash.exceptions import PreventUpdate

from ..utils import adaptive_interval
from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("cpu-util-graph", "figure"),
        Input("interval", "n_intervals"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        Input("queue-dropdown", "value"),
        Input("workspace-store", "data"),
        Input("group-by-toggle", "value"),
    )
    def update_graph(n, start_date, start_time, queue, workspace, group_by):
        try:
            start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        except Exception as e:
            LOG.error(f"Error parsing dates: {e}")
            raise PreventUpdate from e

        end = datetime.utcnow()

        ws_filter = ""
        if workspace:
            ws_filter = f'| where Properties.azureml_workspace_name == "{workspace}"'

        by_env = group_by == "environment"
        dt = adaptive_interval(end - start)

        if by_env:
            query = f"""
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            {ws_filter}
            | where Message startswith "Worker is still running"
            | extend environment=tostring(coalesce(Properties.environment, "<empty>")), CpuUtilization=todecimal(Properties.cpu_util)
            | summarize CpuUtilization=avg(CpuUtilization) by bin(TimeGenerated, {dt}), environment
            | project TimeGenerated, CpuUtilization, environment
            | sort by TimeGenerated asc
            """
            rows = run_query(query)
            df = pd.DataFrame(rows, columns=["TimeGenerated", "CpuUtilization", "environment"])
        else:
            query = f"""
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            {ws_filter}
            | where Message startswith "Worker is still running"
            | extend CpuUtilization=todecimal(Properties.cpu_util)
            | summarize CpuUtilization=avg(CpuUtilization) by bin(TimeGenerated, {dt})
            | project TimeGenerated, CpuUtilization
            | sort by TimeGenerated asc
            """
            rows = run_query(query)
            df = pd.DataFrame(rows, columns=["TimeGenerated", "CpuUtilization"])

        df["CpuUtilization"] = 100.0 * pd.to_numeric(df["CpuUtilization"], errors="coerce")
        fig = px.line(
            df,
            x="TimeGenerated",
            y="CpuUtilization",
            color="environment" if by_env else None,
            title="Average CPU Utilization",
            labels={"CpuUtilization": "CPU Utilization (%)", "TimeGenerated": "Time"},
        )
        fig.update_yaxes(title_text="CPU Utilization (%)", range=[0, 100])
        fig.update_layout(
            title={"x": 0.5, "y": 0.95, "xanchor": "center", "yanchor": "top"},
            margin={"t": 50},
            showlegend=False,
        )
        return fig
