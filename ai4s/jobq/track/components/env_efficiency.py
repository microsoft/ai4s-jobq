# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output
from dash.exceptions import PreventUpdate

from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("env-efficiency-graph", "figure"),
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

        query = f"""
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let Tasks = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where Message startswith "Completed task"
        | extend environment = tostring(Properties.environment)
        | summarize Completed=count() by environment;
        let Preemptions = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where Properties.event == "preemption_detected"
        | extend environment = tostring(Properties.environment)
        | summarize Preemptions=count() by environment;
        Tasks
        | join kind=fullouter Preemptions on environment
        | extend
            Env=coalesce(environment, environment1),
            C=coalesce(Completed, 0),
            P=coalesce(Preemptions, 0)
        | project Env, C, P
        | where P > 0
        | extend Ratio=round(todouble(C) / todouble(P), 2)
        | sort by Ratio desc
        """

        rows = run_query(query)
        if not rows:
            df = pd.DataFrame(columns=["Environment", "Completed", "Preemptions", "Ratio"])
        else:
            df = pd.DataFrame(rows)
            df = df.iloc[:, -4:]
            df.columns = ["Environment", "Completed", "Preemptions", "Ratio"]
        df["Ratio"] = pd.to_numeric(df["Ratio"], errors="coerce").fillna(0)

        n_each = 10
        if len(df) > n_each * 2:
            best = df.head(n_each).copy()
            worst = df.tail(n_each).copy()
            best["Group"] = "best"
            worst["Group"] = "worst"
            df = pd.concat([best, worst]).drop_duplicates(subset="Environment", keep="first")
            title_suffix = f" (top/bottom {n_each})"
        else:
            df["Group"] = "all"
            title_suffix = ""

        # Sort ascending so best ratio at top of horizontal bar
        df = df.sort_values("Ratio", ascending=True)

        color_map = {"best": "#54a24b", "worst": "#e45756", "all": "#4c78a8"}
        colors = df["Group"].map(color_map)

        fig = go.Figure(
            go.Bar(
                x=df["Ratio"],
                y=df["Environment"],
                orientation="h",
                marker_color=colors.tolist(),
            )
        )
        fig.update_layout(
            title={
                "text": f"Tasks per Preemption{title_suffix}",
                "x": 0.5,
                "xanchor": "center",
                "automargin": True,
            },
            margin={"t": 60, "l": 200},
            showlegend=False,
            yaxis_title="",
            xaxis_title="Tasks / Preemption",
        )
        return fig
