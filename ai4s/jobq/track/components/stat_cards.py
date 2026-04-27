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

        # Single query: task totals + worker-days (active time per worker summed)
        query = f"""
        let startTime = datetime({start.isoformat()});
        let endTime = datetime({end.isoformat()});
        let TaskEvents = AppTraces
          | where TimeGenerated between (startTime .. endTime)
          | where Properties.queue == "{queue}"
          {ws_filter}
          | where Message startswith "Completed task" or Message startswith "Failure for task"
          | extend Result = case(
                Message startswith "Failure for task", "Failed",
                "Succeeded"
            )
          | project TimeGenerated, Result;
        let Totals = TaskEvents
        | summarize
            TotalSucceeded=countif(Result == "Succeeded"),
            TotalFailed=countif(Result == "Failed");
        let WorkerDays = AppTraces
        | where TimeGenerated between (startTime .. endTime)
        | where Properties.queue == "{queue}"
        {ws_filter}
        | where isnotempty(Properties.worker_id)
        | summarize
            FirstSeen=min(TimeGenerated),
            LastSeen=max(TimeGenerated)
            by worker_id=tostring(Properties.worker_id)
        | extend ActiveHours = max_of(datetime_diff('minute', LastSeen, FirstSeen), 15) / 60.0
        | summarize TotalWorkerDays = sum(ActiveHours) / 24.0;
        Totals | extend placeholder=1
        | join kind=inner (WorkerDays | extend placeholder=1) on placeholder
        | project TotalSucceeded, TotalFailed, TotalWorkerDays
        """

        rows = run_query(query)
        if rows:
            row = rows[0]
            total_succeeded = int(row[0] or 0)
            total_failed = int(row[1] or 0)
            total_worker_days = float(row[2] or 0)
        else:
            total_succeeded = total_failed = 0
            total_worker_days = 0.0

        days = max(1, (end - start).total_seconds() / 86400)
        avg_succeeded = total_succeeded / days
        avg_failed = total_failed / days

        if total_worker_days > 0:
            succeeded_per_wd = total_succeeded / total_worker_days
            failed_per_wd = total_failed / total_worker_days
        else:
            succeeded_per_wd = failed_per_wd = 0.0

        return _render_stat_cards(
            avg_succeeded,
            avg_failed,
            total_succeeded,
            total_failed,
            days,
            succeeded_per_wd,
            failed_per_wd,
            total_worker_days,
        )


def _render_stat_cards(
    avg_succeeded,
    avg_failed,
    total_succeeded,
    total_failed,
    days,
    succeeded_per_wd,
    failed_per_wd,
    total_worker_days,
):
    """Render the stat cards as Bootstrap-styled cards."""
    card_style = {
        "textAlign": "center",
        "padding": "12px 16px",
        "borderRadius": "8px",
        "minWidth": "140px",
        "flex": "1",
    }

    def stat_card(value, label, color, tooltip=None):
        content = [
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
        ]
        style = {**card_style, "border": f"1px solid {color}20", "background": f"{color}08"}
        if tooltip:
            style["cursor"] = "help"
            return html.Div(content, style=style, title=tooltip)
        return html.Div(content, style=style)

    worker_tooltip = (
        "Tasks per worker-day = total tasks \u00f7 cumulative worker-days. "
        "A worker-day is one worker running for 24h. "
        f"Total worker-days in window: {total_worker_days:.1f}. "
        "Workers with only a single event are counted as 15min active. "
        "Normalizes for workers starting/stopping at different times."
    )

    return html.Div(
        [
            stat_card(avg_succeeded, "Avg succeeded / day", "#28a745"),
            stat_card(avg_failed, "Avg failed / day", "#dc3545"),
            stat_card(
                succeeded_per_wd,
                "Succeeded / worker-day",
                "#17a2b8",
                tooltip=worker_tooltip,
            ),
            stat_card(
                failed_per_wd,
                "Failed / worker-day",
                "#fd7e14",
                tooltip=worker_tooltip,
            ),
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
