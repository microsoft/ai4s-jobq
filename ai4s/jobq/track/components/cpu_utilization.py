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
    )
    def update_graph(n, start_date, start_time, queue):
        try:
            start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        except Exception as e:
            LOG.error(f"Error parsing dates: {e}")
            raise PreventUpdate

        end = datetime.utcnow()

        dt = adaptive_interval(end - start)

        query = f"""
        AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Message startswith "Worker is still running"
            | extend queue=tostring(Properties.queue), environment=tostring(coalesce(Properties.environment, "<empty>")), CpuUtilization=todecimal(Properties.cpu_util)
            | summarize CpuUtilization=avg(CpuUtilization) by bin(TimeGenerated, {dt}), environment
            | project TimeGenerated, CpuUtilization, environment
            | sort by TimeGenerated asc
        """
        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "CpuUtilization", "environment"])
        df["CpuUtilization"] = 100.0 * pd.to_numeric(df["CpuUtilization"], errors="coerce")
        fig = px.line(
            df,
            x="TimeGenerated",
            y="CpuUtilization",
            color="environment",
            title="Average CPU Utilization",
            labels={"CpuUtilization": "CPU Utilization (%)", "TimeGenerated": "Time"},
        )
        fig.update_yaxes(
            title_text="CPU Utilization (%)",
            range=[0, 100],  # Assuming CPU utilization is between 0% and 100%
        )
        fig.update_layout(
            title={
                "x": 0.5,  # Center the title
                "y": 0.95,  # Move the title closer to the graph
                "xanchor": "center",
                "yanchor": "top",
            },
            margin=dict(t=50),  # Adjusted top margin to balance title placement
            showlegend=False,
        )
        return fig
