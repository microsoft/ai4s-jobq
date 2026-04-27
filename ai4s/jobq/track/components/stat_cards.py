# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import datetime

from dash import Input, Output, html
from dash.exceptions import PreventUpdate

from ..utils.log_analytics import run_query

LOG = logging.getLogger(__name__)


def register_callbacks(app):
    @app.callback(
        Output("stat-cards-container", "children"),
        Input("interval", "n_intervals"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        Input("queue-dropdown", "value"),
        Input("workspace-store", "data"),
    )
    def update_stats(n, start_date, start_time, queue, workspace):
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
        let Events = union
        (
          AppTraces
          | where TimeGenerated between (startTime .. endTime)
          | where Properties.queue == "{queue}"
          {ws_filter}
          | where Message startswith "Completed task" or Message startswith "Failure for task"
          | extend Result = case(
                Message startswith "Failure for task", "Failed",
                "Succeeded"
            )
          | project TimeGenerated, Result
        ),
        (
          AppExceptions
          | where TimeGenerated between (startTime .. endTime)
          | where Properties.queue == "{queue}"
          {ws_filter}
          | where Properties.event == "task_failure"
          | extend Result = "Failed"
          | project TimeGenerated, Result
        );
        Events
        | summarize
            Succeeded=countif(Result == "Succeeded"),
            Failed=countif(Result == "Failed")
            by bin(TimeGenerated, 1d)
        | summarize
            AvgSucceeded=avg(Succeeded),
            AvgFailed=avg(Failed),
            TotalSucceeded=sum(Succeeded),
            TotalFailed=sum(Failed)
        """

        rows = run_query(query)
        if rows:
            row = rows[0]
            avg_succeeded = float(row[0] or 0)
            avg_failed = float(row[1] or 0)
            total_succeeded = int(row[2] or 0)
            total_failed = int(row[3] or 0)
        else:
            avg_succeeded = avg_failed = 0.0
            total_succeeded = total_failed = 0

        days = max(1, (end - start).total_seconds() / 86400)

        return _render_stat_cards(avg_succeeded, avg_failed, total_succeeded, total_failed, days)


def _render_stat_cards(avg_succeeded, avg_failed, total_succeeded, total_failed, days):
    """Render the stat cards as Bootstrap-styled cards."""
    card_style = {
        "textAlign": "center",
        "padding": "12px 16px",
        "borderRadius": "8px",
        "minWidth": "140px",
        "flex": "1",
    }

    def stat_card(value, label, color):
        return html.Div(
            [
                html.Div(
                    f"{value:.1f}" if isinstance(value, float) else str(value),
                    style={
                        "fontSize": "28px",
                        "fontWeight": "bold",
                        "color": color,
                        "lineHeight": "1.2",
                    },
                ),
                html.Div(
                    label,
                    style={"fontSize": "12px", "color": "#666", "marginTop": "4px"},
                ),
            ],
            style={**card_style, "border": f"1px solid {color}20", "background": f"{color}08"},
        )

    return html.Div(
        [
            stat_card(avg_succeeded, "Avg succeeded / day", "#28a745"),
            stat_card(avg_failed, "Avg failed / day", "#dc3545"),
            stat_card(total_succeeded, f"Total succeeded ({days:.0f}d)", "#28a745"),
            stat_card(total_failed, f"Total failed ({days:.0f}d)", "#dc3545"),
        ],
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "16px",
        },
    )
