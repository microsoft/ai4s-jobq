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
        Output("env-efficiency-graph", "figure"),
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
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let Tasks = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where Message startswith "Completed task"
        | extend environment = tostring(Properties.environment)
        | summarize Completed=count() by environment;
        let Preemptions = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where Properties.event == "preemption_detected"
        | extend environment = tostring(Properties.environment)
        | summarize Preemptions=count() by environment;
        Tasks
        | join kind=fullouter Preemptions on environment
        | project
            Environment=coalesce(environment, environment1),
            Completed=coalesce(Completed, 0),
            Preemptions=coalesce(Preemptions, 0)
        | where Completed > 0 or Preemptions > 0
        | sort by Completed desc
        """

        rows = run_query(query)
        df = pd.DataFrame(rows, columns=["Environment", "Completed", "Preemptions"])
        df["Completed"] = pd.to_numeric(df["Completed"], errors="coerce").fillna(0)
        df["Preemptions"] = pd.to_numeric(df["Preemptions"], errors="coerce").fillna(0)

        # Efficiency = completions per preemption (higher is better)
        df["Efficiency"] = df["Completed"] / (df["Preemptions"] + 1)

        n_each = 10
        if len(df) > n_each * 2:
            top = df.nlargest(n_each, "Efficiency")
            bottom = df.nsmallest(n_each, "Efficiency")
            top["Group"] = "best"
            bottom["Group"] = "worst"
            df = pd.concat([top, bottom]).drop_duplicates(subset="Environment", keep="first")
            title_suffix = f" (top/bottom {n_each})"
        else:
            df["Group"] = "all"
            title_suffix = ""

        color_map = {"best": "#54a24b", "worst": "#e45756", "all": "#4c78a8"}

        fig = px.scatter(
            df,
            x="Completed",
            y="Preemptions",
            text="Environment",
            color="Group",
            color_discrete_map=color_map,
            size_max=10,
        )
        fig.update_traces(textposition="top center", textfont_size=8)
        fig.update_layout(
            title={
                "text": f"Environment Efficiency{title_suffix}",
                "x": 0.5,
                "y": 0.95,
                "xanchor": "center",
                "yanchor": "top",
            },
            margin={"t": 50},
            showlegend=False,
            xaxis_title="Tasks Completed",
            yaxis_title="Preemptions",
        )
        return fig
