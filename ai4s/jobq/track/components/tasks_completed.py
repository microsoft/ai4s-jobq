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
        Output("tasks-completed-graph", "figure"),
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

        dt = adaptive_interval(end - start)
        query = f"""
        let dt = {dt};
        let successes = AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Properties.event == "task_success"
            | make-series Succeeded=count() default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt
            | mv-expand Succeeded, TimeGenerated
            | extend Succeeded=toint(Succeeded), TimeGenerated=todatetime(TimeGenerated);
        let failures = AppExceptions
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Properties.event == "task_failure"
            | make-series Failed=count() default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt
            | mv-expand Failed, TimeGenerated
            | extend Failed=toint(Failed), TimeGenerated=todatetime(TimeGenerated);
        failures
            | join kind=fullouter successes on TimeGenerated | extend TimeGenerated=iif(isnull(TimeGenerated), TimeGenerated1, TimeGenerated)
            | project TimeGenerated, Failed, Succeeded
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["TimeGenerated", "Failed", "Succeeded"])
        df["Failed"] = pd.to_numeric(df["Failed"], errors="coerce")
        df["Succeeded"] = pd.to_numeric(df["Succeeded"], errors="coerce")
        fig = px.bar(
            df,
            x="TimeGenerated",
            y=["Failed", "Succeeded"],
            title=f"Tasks Completed per {dt}",
            labels={"value": "Count", "variable": "Task Status"},
            color_discrete_sequence=["red", "green"],
            barmode="stack",
        )
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
