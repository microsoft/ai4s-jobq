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
        Output("tasks-completed-graph", "figure"),
        Input("interval", "n_intervals"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        Input("queue-dropdown", "value"),
        Input("workspace-store", "data"),
    )
    def update_graph(n, start_date, start_time, queue, workspace):
        try:
            start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        except Exception as e:
            LOG.error(f"Error parsing dates: {e}")
            raise PreventUpdate from e

        end = datetime.utcnow()

        ws_filter = ""
        if workspace:
            ws_filter = f'| where Properties.azureml_workspace_name == "{workspace}"'

        dt = adaptive_interval(end - start)
        query = f"""
        let dt = {dt};
        let sample_frequency = dt;
        let startTime = floor(datetime({start.isoformat()}), dt);
        let endTime = datetime({end.isoformat()});
        AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where Message startswith "Completed task" or Message startswith "Failure for task"
        | extend Result = case(
              Message startswith "Failure for task", "Failed",
              "Succeeded"
          )
        | project TimeGenerated, Result
        | summarize Succeeded=countif(Result == "Succeeded"), Failed=countif(Result == "Failed") by bin(TimeGenerated, dt)
        | sort by TimeGenerated asc
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "Succeeded", "Failed"])
        df["Failed"] = pd.to_numeric(df["Failed"], errors="coerce")
        df["Succeeded"] = pd.to_numeric(df["Succeeded"], errors="coerce")
        fig = px.bar(
            df,
            x="TimeGenerated",
            y=["Failed", "Succeeded"],
            title=f"Tasks Completed per {dt}",
            labels={"value": "Count", "variable": "Task Status"},
            color_discrete_sequence=["red", "green"],
            barmode="stack",  # type: ignore[arg-type]
        )
        fig.update_layout(
            title={
                "x": 0.5,  # Center the title
                "y": 0.95,  # Move the title closer to the graph
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},  # Adjusted top margin to balance title placement
            showlegend=False,
        )
        return fig
