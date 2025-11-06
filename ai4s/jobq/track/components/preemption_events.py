# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import pandas as pd
import plotly.express as px
from dash import Input, Output
from dash.exceptions import PreventUpdate

from ..utils import adaptive_interval
from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("preemption-events-graph", "figure"),
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

        end = datetime.now()

        dt = adaptive_interval(end - start)

        query = f"""
        let dt = {dt};
        AppTraces
        | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
        | where Properties.queue == "{queue}"
        | where Message startswith "Preemption event detected"
        | project environment=tostring(Properties.environment), TimeGenerated
        | make-series NumEvents=count() default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt by environment
| mv-expand NumEvents, TimeGenerated
        | project TimeGenerated=todatetime(TimeGenerated), environment=tostring(environment), NumEvents=todecimal(NumEvents)
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "environment", "NumEvents"])
        df["NumEvents"] = pd.to_numeric(df["NumEvents"], errors="coerce")
        fig = px.bar(
            df,
            x="TimeGenerated",
            y="NumEvents",
            color="environment",
            title=f"Preemptions per {dt}",
        )
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
