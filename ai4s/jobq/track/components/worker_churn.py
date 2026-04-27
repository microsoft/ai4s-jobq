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
        Output("worker-churn-graph", "figure"),
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
        let dt = 15m;
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let WorkerLifetimes = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where isnotempty(Properties.worker_id)
        | summarize FirstSeen=min(TimeGenerated), LastSeen=max(TimeGenerated)
            by worker_id=tostring(Properties.worker_id);
        let Arrivals = WorkerLifetimes
        | where FirstSeen >= startTime
        | summarize Arrived=count() by TimeBin=bin(FirstSeen, dt);
        let Departures = WorkerLifetimes
        | where LastSeen < endTime - dt
        | summarize Departed=count() by TimeBin=bin(LastSeen, dt);
        Arrivals
        | join kind=fullouter Departures on TimeBin
        | extend
            T=coalesce(TimeBin, TimeBin1),
            A=coalesce(Arrived, 0),
            D=coalesce(Departed, 0)
        | project T, A, D
        | sort by T asc
        """

        rows = run_query(query)
        if not rows:
            df = pd.DataFrame(columns=["TimeBin", "Arrived", "Departed"])
        else:
            df = pd.DataFrame(rows)
            # fullouter may return 3-5 columns; we only need the last 3 projected
            df = df.iloc[:, -3:]
            df.columns = ["TimeBin", "Arrived", "Departed"]
        df["Arrived"] = pd.to_numeric(df["Arrived"], errors="coerce").fillna(0)
        df["Departed"] = pd.to_numeric(df["Departed"], errors="coerce").fillna(0)

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=df["TimeBin"],
                y=df["Arrived"],
                name="Arrived",
                marker_color="#28a745",
            )
        )
        fig.add_trace(
            go.Bar(
                x=df["TimeBin"],
                y=-df["Departed"],
                name="Departed",
                marker_color="#dc3545",
            )
        )
        fig.update_layout(
            title={
                "text": "Worker Churn",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},
            barmode="relative",
            showlegend=False,
            yaxis_title="Workers",
        )
        return fig
