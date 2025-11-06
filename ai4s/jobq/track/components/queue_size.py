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
        Output("queue-size-graph", "figure"),
        Input("interval", "n_intervals"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        Input("queue-dropdown", "value"),
    )
    def update_graph(n, start_date, start_time, queue):
        try:
            start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        except Exception as e:
            LOG.error(f"Error parsing dates: {e}")
            raise PreventUpdate

        end = datetime.utcnow()

        query = f"""
        let dt = 15m;
        AppTraces
        | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
        | where Properties.queue == "{queue}"
        | where Message startswith "Worker is still running"
        | project queue=tostring(Properties.queue), queue_size=todecimal(Properties.queue_size), TimeGenerated
        | make-series QueueSize=avg(queue_size) default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt
        | mv-expand TimeGenerated, QueueSize
        | project TimeGenerated=todatetime(TimeGenerated), QueueSize=todecimal(QueueSize)
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "QueueSize"])
        df["QueueSize"] = pd.to_numeric(df["QueueSize"], errors="coerce")
        max_y = df["QueueSize"].max()
        fig = px.line(df, x="TimeGenerated", y="QueueSize", title="Queue Size")
        if max_y > 0:
            fig.update_yaxes(range=[0, max_y * 1.1])
        fig.update_yaxes(tickformat=".0f")
        fig.update_layout(
            title={
                "x": 0.5,  # Center the title
                "y": 0.95,  # Move the title closer to the graph
                "xanchor": "center",
                "yanchor": "top",
            },
            margin=dict(t=50),  # Adjusted top margin to balance title placement
            showlegend=False,
        )
        return fig
