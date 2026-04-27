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
        Output("task-runtimes-graph", "figure"),
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
        AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
        {ws_filter}
            | where Message startswith "Completed"
            | extend runtime=todecimal(Properties.duration_s)
            | summarize min=min(runtime), q25=percentile(runtime, 25), median=percentile(runtime, 50), q75=percentile(runtime, 75), max=max(runtime) , mean=avg(runtime) by bin(TimeGenerated, {dt})
            | sort by TimeGenerated asc
        """
        rows = run_query(query)
        df = pd.DataFrame(
            rows,
            columns=["TimeGenerated", "min", "q25", "median", "q75", "max", "mean"],
        )
        for col in ["min", "q25", "median", "q75", "max", "mean"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        x = df["TimeGenerated"]
        band_color = "44, 127, 184"  # teal-blue base

        fig = go.Figure()

        # Band: min-max (lightest)
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["max"],
                mode="lines",
                line={"width": 0},
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["min"],
                mode="lines",
                line={"width": 0},
                fill="tonexty",
                fillcolor=f"rgba({band_color}, 0.12)",
                name="min-max",
                hoverinfo="skip",
            )
        )

        # Band: q25-q75 (more opaque)
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["q75"],
                mode="lines",
                line={"width": 0},
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["q25"],
                mode="lines",
                line={"width": 0},
                fill="tonexty",
                fillcolor=f"rgba({band_color}, 0.30)",
                name="p25-p75",
                hoverinfo="skip",
            )
        )

        # Line: median (solid, most prominent)
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["median"],
                mode="lines+markers",
                line={"color": f"rgb({band_color})", "width": 2},
                marker={"size": 3},
                name="median",
            )
        )

        # Line: mean (dashed)
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df["mean"],
                mode="lines",
                line={"color": "#333", "width": 1.5, "dash": "dash"},
                name="mean",
            )
        )

        # Format y-axis
        fig.update_yaxes(
            type="log",
            tickvals=[1, 10, 60, 300, 600, 3600, 18000, 36000, 86400, 432000],
            ticktext=["1s", "10s", "1m", "5m", "10m", "1h", "5h", "10h", "1d", "5d"],
            title="Duration",
        )

        fig.update_layout(
            title={
                "text": "Task Runtimes",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},
            showlegend=False,
        )
        return fig
