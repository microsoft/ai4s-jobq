# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output
from dash.exceptions import PreventUpdate

from ..utils import adaptive_interval
from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("tasks-completed-trend-graph", "figure"),
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
        df = pd.DataFrame(rows, columns=["TimeGenerated", "Failed", "Succeeded"])
        df["Failed"] = pd.to_numeric(df["Failed"], errors="coerce").fillna(0)
        df["Succeeded"] = pd.to_numeric(df["Succeeded"], errors="coerce").fillna(0)
        df = df.sort_values("TimeGenerated").reset_index(drop=True)

        # Compute rolling 24h moving average (window size depends on bin interval)
        interval_hours = _interval_to_hours(dt)
        window_size = max(1, int(24 / interval_hours))

        df["Succeeded_MA"] = df["Succeeded"].rolling(window=window_size, min_periods=1).mean()
        df["Failed_MA"] = df["Failed"].rolling(window=window_size, min_periods=1).mean()

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df["TimeGenerated"],
                y=df["Succeeded_MA"],
                mode="lines",
                name="Succeeded (24h avg)",
                line={"color": "green", "width": 2},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df["TimeGenerated"],
                y=df["Failed_MA"],
                mode="lines",
                name="Failed (24h avg)",
                line={"color": "red", "width": 2},
            )
        )

        fig.update_layout(
            title={
                "text": "Daily Moving Average of Tasks Completed",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},
            xaxis_title="Time",
            yaxis_title="Tasks / interval (smoothed)",
            showlegend=True,
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        )
        return fig


def _interval_to_hours(dt_str: str) -> float:
    """Convert an adaptive interval string like '15m', '2h' to hours."""
    if dt_str.endswith("m"):
        return int(dt_str[:-1]) / 60
    if dt_str.endswith("h"):
        return int(dt_str[:-1])
    return 1
