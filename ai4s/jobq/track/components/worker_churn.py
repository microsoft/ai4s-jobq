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

        ws_filter = ""
        if workspace:
            ws_filter = f'| where Properties.azureml_workspace_name == "{workspace}"'

        query = f"""
        let dt = 15m;
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where isnotempty(Properties.worker_id)
        | summarize FirstSeen=min(TimeGenerated), LastSeen=max(TimeGenerated)
            by worker_id=tostring(Properties.worker_id)
        | extend ArrivalBin=bin(FirstSeen, dt), DepartureBin=bin(LastSeen, dt)
        | project ArrivalBin, DepartureBin
        | as WorkerLifetimes;
        let Arrivals = WorkerLifetimes
        | summarize Arrived=count() by TimeBin=ArrivalBin;
        let Departures = WorkerLifetimes
        | summarize Departed=count() by TimeBin=DepartureBin;
        Arrivals
        | join kind=fullouter Departures on TimeBin
        | project
            TimeBin=coalesce(TimeBin, TimeBin1),
            Arrived=coalesce(Arrived, 0),
            Departed=coalesce(Departed, 0)
        | sort by TimeBin asc
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeBin", "Arrived", "Departed"])
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
