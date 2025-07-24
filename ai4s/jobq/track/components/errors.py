import logging
from datetime import datetime

import numpy as np
import pandas as pd
from dash import Input, Output
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

        query = f"""
        AppExceptions
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            | where InnermostMessage has "Failure"
            | extend TaskId = tostring(Properties.task_id),
                     Duration=todecimal(Properties.duration_s),
                     Exception = extract(@"raise .*\\n(.*)", 1, tostring(Properties.log))
            | project TimeGenerated, TaskId, Duration, Exception, Logs=tostring(Properties.log), ExceptionType
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
            ],
        )

        df["TimeGenerated"] = pd.to_datetime(df["TimeGenerated"]).dt.strftime("%Y-%m-%d %H:%M")
        df["Duration"] = df["Duration"].astype(float)
        df["Exception"].replace("", np.nan, inplace=True)
        df["Exception"] = df["Exception"].fillna(df["ExceptionType"])
        df.drop(columns=["ExceptionType"], inplace=True)

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
            {"Logs": {"type": "text", "value": row["Logs"]}} for _, row in df.iterrows()
        ]

        return df.to_dict("records"), columns, tooltip_data
