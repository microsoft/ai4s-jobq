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
        Output("active-environments-graph", "figure"),
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
        AppTraces
        | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
        | where Properties.queue == "{queue}"
        | where isnotempty(Properties.environment)
        | summarize ActiveEnvironments=dcount(tostring(Properties.environment))
            by bin(TimeGenerated, dt)
        | sort by TimeGenerated asc
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "ActiveEnvironments"])
        df["ActiveEnvironments"] = pd.to_numeric(df["ActiveEnvironments"], errors="coerce")
        fig = px.area(df, x="TimeGenerated", y="ActiveEnvironments", title="Active Environments")
        fig.update_layout(
            title={"x": 0.5, "y": 0.95, "xanchor": "center", "yanchor": "top"},
            margin={"t": 50},
            showlegend=False,
            yaxis={"rangemode": "tozero"},
        )
        return fig
