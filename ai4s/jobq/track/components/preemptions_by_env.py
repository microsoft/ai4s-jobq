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
        Output("preemptions-by-env-graph", "figure"),
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

        n_each = 10
        # Avg time from worker first-seen to preemption, per environment.
        # Workers that were never preempted are excluded.
        query = f"""
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let WorkerStarts = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where isnotempty(Properties.worker_id)
        | summarize WorkerStart=min(TimeGenerated)
            by worker_id=tostring(Properties.worker_id),
               environment=tostring(Properties.environment);
        let PreemptionTimes = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where Properties.event == "preemption_detected"
        | extend worker_id=tostring(Properties.worker_id),
                 environment=tostring(Properties.environment)
        | summarize FirstPreemption=min(TimeGenerated) by worker_id, environment;
        WorkerStarts
        | join kind=inner PreemptionTimes on worker_id, environment
        | extend MinutesToPreemption=datetime_diff('second', FirstPreemption, WorkerStart) / 60.0
        | where MinutesToPreemption >= 0
        | summarize AvgMinutes=avg(MinutesToPreemption), Workers=count() by environment
        | where Workers >= 3
        | sort by AvgMinutes asc;
        """

        rows = run_query(query)
        if not rows:
            df = pd.DataFrame(columns=["Environment", "AvgMinutes", "Workers"])
        else:
            df = pd.DataFrame(rows, columns=["Environment", "AvgMinutes", "Workers"])
        df["AvgMinutes"] = pd.to_numeric(df["AvgMinutes"], errors="coerce").fillna(0)

        # Show top N shortest (worst) and top N longest (best)
        if len(df) > n_each * 2:
            worst = df.head(n_each).copy()
            best = df.tail(n_each).copy()
            worst["Group"] = "shortest"
            best["Group"] = "longest"
            df = pd.concat([worst, best]).drop_duplicates(subset="Environment", keep="first")
        else:
            df["Group"] = "all"

        # Sort ascending so longest at top of horizontal bar
        df = df.sort_values("AvgMinutes", ascending=True)

        color_map = {"shortest": "#e45756", "longest": "#54a24b", "all": "#4c78a8"}
        colors = df["Group"].map(color_map)

        fig = go.Figure(
            go.Bar(
                x=df["AvgMinutes"],
                y=df["Environment"],
                orientation="h",
                marker_color=colors.tolist(),
            )
        )
        fig.update_layout(
            title={
                "text": f"Avg Time to Preemption (top/bottom {n_each})",
                "x": 0.5,
                "y": 0.99,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 70, "l": 200},
            showlegend=False,
            yaxis_title="",
            xaxis_title="Minutes",
        )
        return fig
