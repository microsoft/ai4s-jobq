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
        Output("task-runtimes-graph", "figure"),
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
        AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where Message startswith "Completed"
            | extend runtime=todecimal(Properties.duration_s)
            | summarize min=min(runtime), q25=percentile(runtime, 25), median=percentile(runtime, 50), q75=percentile(runtime, 75), max=max(runtime) , mean=avg(runtime) by bin(TimeGenerated, {dt})
            | sort by TimeGenerated asc
        """
        rows = run_query(query)
        df = pd.DataFrame(
            rows,
            columns=["TimeGenerated", "min", "q25", "median", "q75", "max", "mean"],
        )
        for col in ["min", "q25", "median", "q75", "max", "mean"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df_long = df.melt(id_vars="TimeGenerated", var_name="statistic", value_name="value")
        # solarized, with min/max in gray, q25/q75 in cyan, median in red, mean in black
        color_map = {
            "min": "#444444",  # gray
            "q25": "#2aa198",  # cyan
            "median": "#dc322f",  # red
            "q75": "#2aa198",  # cyan
            "max": "#888888",  # gray
            "mean": "#000000",  # black
        }
        fig = px.line(
            df_long,
            x="TimeGenerated",
            y="value",
            color="statistic",
            color_discrete_map=color_map,
            labels={"TimeGenerated": "Time", "value": "Runtime (seconds)"},
            title="Task Runtimes",
            markers=True,
        )

        # Format y-axis
        fig.update_yaxes(
            type="log",
            tickvals=[1, 10, 60, 300, 600, 3600, 18000, 36000, 86400, 432000],
            ticktext=["1s", "10s", "1m", "5m", "10m", "1h", "5h", "10h", "1d", "5d"],
            title="Duration",
        )

        for trace in fig.data:
            if trace.name not in ("median", "mean"):
                trace.update(mode="lines")

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
