# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

import dash
import numpy as np
import pandas as pd
from dash import Input, Output, State
from dash.dash_table.Format import Format, Scheme
from dash.exceptions import PreventUpdate

from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("errors-table", "data"),
        Output("errors-table", "columns"),
        Output("errors-table", "tooltip_data"),
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

        TID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
        query = f"""
        AppExceptions
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where InnermostMessage has "Failure"
            | extend TaskId = tostring(Properties.task_id),
                     Duration=todecimal(Properties.duration_s),
                     URL=strcat("https://ml.azure.com/runs/", Properties.azureml_run_id, "?tid={TID}&wsid=", "/subscriptions/", Properties.azureml_subscription_id, "/resourceGroups/", Properties.azureml_resource_group, "/workspaces/", Properties.azureml_workspace_name),
                     Exception = extract(@"raise .*\\n(.*)", 1, tostring(Properties.log))
            | project TimeGenerated, TaskId, Duration, Exception, Logs=tostring(Properties.log), ExceptionType, URL
            | sort by TimeGenerated desc
            | limit 100
        """

        rows = run_query(query)
        df = pd.DataFrame(
            rows,
            columns=[
                "TimeGenerated",
                "TaskId",
                "Duration",
                "Exception",
                "Logs",
                "ExceptionType",
                "URL",
            ],
        )

        df["TimeGenerated"] = pd.to_datetime(df["TimeGenerated"]).dt.strftime("%Y-%m-%d %H:%M")
        df["Duration"] = df["Duration"].astype(float)
        df["Exception"].replace("", np.nan, inplace=True)
        df["Exception"] = df["Exception"].fillna(df["ExceptionType"])
        df["TaskId"] = df.apply(lambda row: f"[{row['TaskId']}]({row['URL']})", axis=1)
        df.drop(columns=["ExceptionType", "URL"], inplace=True)

        columns = [
            {"name": "Time", "id": "TimeGenerated", "type": "datetime"},
            {
                "name": "Task ID",
                "id": "TaskId",
                "type": "text",
                "presentation": "markdown",
            },
            {
                "name": "Duration (s)",
                "id": "Duration",
                "type": "numeric",
                "format": Format(scheme=Scheme.fixed, precision=0),
            },
            {"name": "Exception", "id": "Exception"},
            {"name": "Logs", "id": "Logs"},
        ]

        tooltip_data = [
            # {"Logs": {"type": "markdown", "value": ("<pre>" + row["Logs"] + "</pre>")}} for _, row in df.iterrows()
        ]

        return df.to_dict("records"), columns, tooltip_data

    @app.callback(
        Output("modal", "is_open"),
        Output("modal-body", "children"),
        Output("copy-to-clipboard", "data"),
        Input("errors-table", "active_cell"),
        Input("close-error-details", "n_clicks"),
        State("modal", "is_open"),
        State("errors-table", "data"),
        prevent_initial_call=True,
    )
    def display_modal(active_cell, close_click, is_open, data):
        ctx = dash.callback_context

        if not ctx.triggered:
            return dash.no_update, dash.no_update, dash.no_update

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "close-error-details":
            return False, dash.no_update, dash.no_update

        if trigger_id == "errors-table" and active_cell:
            row = active_cell["row"]
            col = active_cell["column_id"]
            if col == "Logs":
                if data[row][col].strip():
                    return True, data[row][col], data[row][col]
                else:
                    return False, dash.no_update, dash.no_update
            else:
                return dash.no_update, dash.no_update, data[row][col]

        return False, dash.no_update, dash.no_update

    @app.callback(
        Output("modal-body", "style"),
        Input("wrap-toggle", "value"),
        State("modal-body", "style"),
    )
    def toggle_wrap(wrap_enabled, current_style):
        style = dict(
            userSelect="text",
            overflowX="auto",
            padding="1rem",
            fontFamily="monospace",
        )
        if wrap_enabled:
            style.update(
                dict(
                    whiteSpace="pre-wrap",
                    overflowX="visible",
                    overflowWrap="break-word",
                    wordBreak="break-word",
                )
            )
        else:
            style.update(
                dict(
                    whiteSpace="pre",
                    overflowX="auto",
                    overflowWrap="normal",
                    wordBreak="normal",
                )
            )
        current_style.update(style)
        return current_style

    app.clientside_callback(
        """
        function(value) {
            if (!value) return null;

            navigator.clipboard.writeText(value).then(() => {
                const toast = document.getElementById('copy-toast');
                if (toast) {
                    toast.innerText = 'Copied!';
                    toast.style.display = 'block';
                    setTimeout(() => {
                        toast.style.display = 'none';
                    }, 1500);
                }
            });

            return null;
        }
        """,
        Output("clipboard-data", "data"),
        Input("copy-to-clipboard", "data"),
    )
