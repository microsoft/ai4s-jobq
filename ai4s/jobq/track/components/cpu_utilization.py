# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

        by_env = group_by == "environment"
        dt = adaptive_interval(end - start)

        if by_env:
            query = f"""
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Message startswith "Worker is still running"
            | extend environment=tostring(coalesce(Properties.environment, "<empty>")), CpuUtilization=todecimal(Properties.cpu_util)
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
        else:
            query = f"""
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Message startswith "Worker is still running"
            | extend CpuUtilization=todecimal(Properties.cpu_util)
            | summarize
                p10=percentile(CpuUtilization, 10),
                p25=percentile(CpuUtilization, 25),
                avg_val=avg(CpuUtilization),
                p75=percentile(CpuUtilization, 75),
                p90=percentile(CpuUtilization, 90)
                by bin(TimeGenerated, {dt})
            | sort by TimeGenerated asc
            """
            rows = run_query(query)
            df = pd.DataFrame(
                rows, columns=["TimeGenerated", "p10", "p25", "avg_val", "p75", "p90"]
            )
            for col in ["p10", "p25", "avg_val", "p75", "p90"]:
                df[col] = 100.0 * pd.to_numeric(df[col], errors="coerce")

            band_color = "76, 120, 168"
            fig = go.Figure()
            # p10-p90 band
            fig.add_trace(
                go.Scatter(
                    x=df["TimeGenerated"],
                    y=df["p90"],
                    mode="lines",
                    line={"width": 0},
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["TimeGenerated"],
                    y=df["p10"],
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor=f"rgba({band_color}, 0.12)",
                    name="p10-p90",
                    hoverinfo="skip",
                )
            )
            # p25-p75 band
            fig.add_trace(
                go.Scatter(
                    x=df["TimeGenerated"],
                    y=df["p75"],
                    mode="lines",
                    line={"width": 0},
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["TimeGenerated"],
                    y=df["p25"],
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor=f"rgba({band_color}, 0.25)",
                    name="p25-p75",
                    hoverinfo="skip",
                )
            )
            # Mean line
            fig.add_trace(
                go.Scatter(
                    x=df["TimeGenerated"],
                    y=df["avg_val"],
                    mode="lines",
                    line={"color": f"rgb({band_color})", "width": 2},
                    name="mean",
                )
            )
            fig.update_layout(title="Average CPU Utilization")

        fig.update_yaxes(title_text="CPU Utilization (%)", range=[0, 100])
        fig.update_layout(
            title={"x": 0.5, "y": 0.95, "xanchor": "center", "yanchor": "top"},
            margin={"t": 50},
            showlegend=False,
        )
        return fig
