# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.express as px
from dash import Input, Output
from dash.exceptions import PreventUpdate

from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("worker-lifetime-graph", "figure"),
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
        AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        | where isnotempty(Properties.worker_id)
        | summarize FirstSeen=min(TimeGenerated), LastSeen=max(TimeGenerated)
            by worker_id=tostring(Properties.worker_id)
        | extend LifetimeMinutes=datetime_diff('minute', LastSeen, FirstSeen)
        | extend LifetimeMinutes=max_of(LifetimeMinutes, 1)
        | project LifetimeMinutes
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["LifetimeMinutes"])
        df["LifetimeMinutes"] = pd.to_numeric(df["LifetimeMinutes"], errors="coerce").fillna(0)

        fig = px.histogram(
            df,
            x="LifetimeMinutes",
            nbins=50,
            labels={"LifetimeMinutes": "Lifetime (minutes)"},
            color_discrete_sequence=["#4c78a8"],
        )
        fig.update_layout(
            title={
                "text": "Worker Lifetime Distribution",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},
            showlegend=False,
            yaxis_title="Workers",
            xaxis_title="Lifetime (minutes)",
            bargap=0.05,
        )
        return fig
