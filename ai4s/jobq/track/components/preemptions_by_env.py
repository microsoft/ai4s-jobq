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

        ws_filter = ""
        if workspace:
            ws_filter = f'| where Properties.azureml_workspace_name == "{workspace}"'

        n_each = 10
        query = f"""
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let All = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where Properties.event == "preemption_detected"
        | extend environment = tostring(Properties.environment)
        | summarize Preemptions=count() by environment
        | sort by Preemptions desc;
        let Top = All | take {n_each};
        let Bottom = All | sort by Preemptions asc | take {n_each};
        Top | extend Group="worst"
        | union (Bottom | extend Group="best")
        | sort by Preemptions desc
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["Environment", "Preemptions", "Group"])
        df["Preemptions"] = pd.to_numeric(df["Preemptions"], errors="coerce").fillna(0)
        # De-duplicate environments that appear in both top and bottom
        df = df.drop_duplicates(subset="Environment", keep="first")
        # Reverse for horizontal bar (top item at top)
        df = df.iloc[::-1]

        colors = df["Group"].map({"worst": "#e45756", "best": "#54a24b"})

        fig = go.Figure(
            go.Bar(
                x=df["Preemptions"],
                y=df["Environment"],
                orientation="h",
                marker_color=colors.tolist(),
            )
        )
        fig.update_layout(
            title={
                "text": f"Preemptions by Environment (top/bottom {n_each})",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50, "l": 150},
            showlegend=False,
            yaxis_title="",
            xaxis_title="Preemptions",
        )
        return fig
