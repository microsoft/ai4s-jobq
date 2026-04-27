# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
import urllib.parse
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, dash_table, dcc, html
from dash.exceptions import PreventUpdate

from ..utils.log_analytics import get_distinct_values, run_query

LOG = logging.getLogger(__name__)

_label_style = {"fontSize": "11px", "color": "#888", "marginBottom": "2px", "display": "block"}


def layout(default_queue=None):
    now = datetime.utcnow()
    default_start = now - timedelta(hours=24)

    queue_options = [{"label": q, "value": q} for q in get_distinct_values("Properties.queue")]
    if default_queue and default_queue not in [q["value"] for q in queue_options]:
        queue_options.insert(0, {"label": default_queue, "value": default_queue})

    if default_queue:
        default_value = default_queue
    elif queue_options:
        default_value = queue_options[0]["value"]
    else:
        default_value = "unknown/queue"

    return html.Div(
        [
            dcc.Location(id="url", refresh=False),
            html.Span(id="current-queue", style={"display": "none"}),
            dcc.Store(id="workspace-store", data=""),
            html.Div(
                [
                    # Row 1: Queue selector + workspace
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Queue", style=_label_style),
                                    dcc.Dropdown(
                                        id="queue-dropdown",
                                        options=queue_options or [],  # type: ignore[arg-type]
                                        value=default_value,
                                        placeholder="Select a queue",
                                    ),
                                ],
                                style={"flex": "1", "minWidth": "250px"},
                            ),
                            html.Div(
                                [
                                    html.Label("Workspace", style=_label_style),
                                    html.Div(
                                        id="workspace-display",
                                        children="detecting…",
                                        style={
                                            "fontSize": "13px",
                                            "color": "#555",
                                            "padding": "6px 0",
                                        },
                                    ),
                                ],
                                style={"marginLeft": "24px", "minWidth": "120px"},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "flex-end",
                            "marginBottom": "10px",
                        },
                    ),
                    # Row 2: Time range + refresh
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Time range", style=_label_style),
                                    dbc.RadioItems(
                                        id="time-range-preset",
                                        options=[
                                            {"label": "6h", "value": "6h"},
                                            {"label": "12h", "value": "12h"},
                                            {"label": "24h", "value": "24h"},
                                            {"label": "3d", "value": "3d"},
                                            {"label": "7d", "value": "7d"},
                                        ],
                                        value="24h",
                                        inline=True,
                                        className="btn-group",
                                        inputClassName="btn-check",
                                        labelClassName="btn btn-outline-secondary btn-sm",
                                        labelCheckedClassName="active",
                                    ),
                                ],
                                style={"marginRight": "24px"},
                            ),
                            html.Div(
                                [
                                    html.Label("From", style=_label_style),
                                    html.Div(
                                        [
                                            dcc.DatePickerSingle(
                                                id="date-picker-single",
                                                date=default_start.date().strftime("%Y-%m-%d"),
                                                display_format="YYYY-MM-DD",
                                            ),
                                            dcc.Input(
                                                id="start-time",
                                                type="text",
                                                value=default_start.strftime("%H:%M"),
                                                placeholder="HH:MM",
                                                className="form-control form-control-sm",
                                                style={"width": "70px", "marginLeft": "6px"},
                                            ),
                                        ],
                                        style={"display": "flex", "alignItems": "center"},
                                    ),
                                ],
                                style={"marginRight": "24px"},
                            ),
                            html.Div(
                                [
                                    html.Label("Refresh", style=_label_style),
                                    dcc.Dropdown(
                                        id="refresh-interval-dropdown",
                                        options=[  # type: ignore[arg-type]
                                            {"label": "Off", "value": 0},
                                            {"label": "30s", "value": 30000},
                                            {"label": "1m", "value": 60000},
                                            {"label": "5m", "value": 300000},
                                        ],
                                        value=60000,
                                        clearable=False,
                                        style={"width": "80px"},
                                    ),
                                ],
                                style={"marginRight": "24px"},
                            ),
                            html.Div(
                                [
                                    html.Label("Group by", style=_label_style),
                                    dbc.RadioItems(
                                        id="group-by-toggle",
                                        options=[
                                            {"label": "Overall", "value": "overall"},
                                            {"label": "Environment", "value": "environment"},
                                        ],
                                        value="overall",
                                        inline=True,
                                        className="btn-group",
                                        inputClassName="btn-check",
                                        labelClassName="btn btn-outline-secondary btn-sm",
                                        labelCheckedClassName="active",
                                    ),
                                ],
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "flex-end",
                            "flexWrap": "wrap",
                            "gap": "4px",
                        },
                    ),
                ],
                style={
                    "padding": "12px 16px",
                    "marginBottom": "16px",
                    "borderBottom": "1px solid #dee2e6",
                },
            ),
            html.Div(id="stat-cards-container", style={"marginBottom": "8px"}),
            html.Div(
                [
                    dcc.Graph(
                        id="active-workers-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="queue-size-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="tasks-starting-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="tasks-completed-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="task-runtimes-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="cpu-util-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="ram-util-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="preemption-events-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                    dcc.Graph(
                        id="tasks-completed-trend-graph",
                        style={
                            "aspectRatio": "12 / 9",
                            "margin": "5px",
                            "minWidth": "400px",
                        },
                    ),
                ],
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(400px, 1fr))",
                    "gap": "10px",  # Adds spacing between grid items
                },
            ),
            html.H2("Errors"),
            html.Div(
                id="copy-toast",
                style={
                    "position": "fixed",
                    "bottom": "20px",
                    "left": "20px",
                    "backgroundColor": "#ff0000",
                    "color": "white",
                    "padding": "10px 15px",
                    "borderRadius": "5px",
                    "display": "none",
                    "zIndex": 9999,
                    "boxShadow": "0 2px 6px rgba(0,0,0,0.3)",
                    "fontSize": "14px",
                },
            ),
            html.Div(
                [
                    dash_table.DataTable(  # type: ignore[attr-defined]
                        id="errors-table",
                        style_table={"width": "100%", "overflowX": "hidden"},
                        style_cell_conditional=[
                            {
                                "if": {"column_id": "TimeGenerated"},
                                "width": "150px",
                                "minWidth": "150px",
                                "maxWidth": "150px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            {
                                "if": {"column_id": "TaskId"},
                                "width": "250px",
                                "minWidth": "150px",
                                "maxWidth": "300px",
                                "overflow": "hidden",
                                "presentation": "markdown",
                                "textOverflow": "ellipsis",
                            },
                            {
                                "if": {"column_id": "Duration"},
                                "width": "50px",
                                "minWidth": "50px",
                                "maxWidth": "150px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            {
                                "if": {"column_id": "Exception"},
                                "width": "50px",
                                "minWidth": "50px",
                                "maxWidth": "150px",
                                "textAlign": "left",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            {
                                "if": {"column_id": "Logs"},
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                                "textAlign": "left",
                                "whiteSpace": "nowrap",
                            },
                        ],
                    ),
                ],
                style={"zIndex": 1},
            ),
            dcc.Store(id="copy-to-clipboard"),
            dcc.Store(id="clipboard-data"),
            dbc.Modal(
                [
                    dbc.ModalHeader("Error Log"),
                    html.Div(
                        dbc.ModalBody(
                            html.Pre(
                                id="modal-body", style={"maxHeight": "60vh", "overflowY": "auto"}
                            )
                        ),
                        id="modal-body-div",
                        # text selection pointer
                        style={"cursor": "text", "width": "100%"},
                    ),
                    dbc.ModalFooter(
                        [
                            dbc.Button("Close", id="close-error-details", className="ml-auto"),
                            dbc.Switch(
                                id="wrap-toggle", label="Wrap text", value=True, persistence=True
                            ),
                        ]
                    ),
                ],
                id="modal",
                is_open=False,
                centered=True,
                backdrop="true",
                scrollable=True,
                size="lg",
                style={"zIndex": 1050, "maxWidth": "800px", "width": "90%"},
            ),
            dcc.Interval(id="interval", interval=60 * 1000, n_intervals=0),
            dcc.Interval(id="resize-interval", interval=500, n_intervals=0),
            html.Script(
                """
                window.addEventListener('resize', function() {
                    const event = new Event('resize');
                    window.dispatchEvent(event);
                });
                """,
                type="text/javascript",
            ),
        ]
    )


def register_callbacks(app):
    @app.callback(
        Output("active-workers-graph", "figure"),
        Output("current-queue", "children"),
        Input("interval", "n_intervals"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        Input("queue-dropdown", "value"),
        Input("workspace-store", "data"),
        Input("group-by-toggle", "value"),
    )
    def update_graph(n, start_date, start_time, queue, workspace, group_by):
        try:
            start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        except Exception as e:
            LOG.error(f"Error parsing dates: {e}")
            raise PreventUpdate from e

        end = datetime.utcnow()

        ws_filter = ""
        if workspace:
            ws_filter = f'| where Properties.azureml_workspace_name == "{workspace}"'

        by_env = group_by == "environment"
        dt = "15m"

        if by_env:
            query = f"""
            let dt = {dt};
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            {ws_filter}
            | where Message startswith "Worker is still running"
            | project environment=tostring(coalesce(Properties.environment, "<empty>")), worker_id=tostring(Properties.worker_id), TimeGenerated
            | make-series ActiveWorkers=count_distinct(worker_id) default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt by environment
            | mv-expand TimeGenerated, ActiveWorkers
            | project TimeGenerated=todatetime(TimeGenerated), environment=tostring(environment), ActiveWorkers=todecimal(ActiveWorkers)
            """
        else:
            query = f"""
            let dt = {dt};
            AppTraces
            | where TimeGenerated between (datetime({start.isoformat()}) .. datetime({end.isoformat()}))
            | where Properties.queue == "{queue}"
            {ws_filter}
            | where Message startswith "Worker is still running"
            | project worker_id=tostring(Properties.worker_id), TimeGenerated
            | make-series ActiveWorkers=count_distinct(worker_id) default=0 on TimeGenerated from floor(datetime({start.isoformat()}), dt) to floor(datetime({end.isoformat()}), dt) step dt
            | mv-expand TimeGenerated, ActiveWorkers
            | project TimeGenerated=todatetime(TimeGenerated), ActiveWorkers=todecimal(ActiveWorkers)
            """

        rows = run_query(query)
        if by_env:
            df = pd.DataFrame(rows, columns=["TimeGenerated", "environment", "ActiveWorkers"])
            fig = px.area(
                df,
                x="TimeGenerated",
                y="ActiveWorkers",
                color="environment",
                title="Active Workers",
                groupnorm=None,
            )
        else:
            df = pd.DataFrame(rows, columns=["TimeGenerated", "ActiveWorkers"])
            fig = px.area(df, x="TimeGenerated", y="ActiveWorkers", title="Active Workers")

        fig.update_layout(
            title={"x": 0.5, "y": 0.95, "xanchor": "center", "yanchor": "top"},
            margin={"t": 50},
            showlegend=by_env,
        )

        df["ActiveWorkers"] = pd.to_numeric(df["ActiveWorkers"], errors="coerce")
        max_y = df["ActiveWorkers"].max()
        if max_y > 0:
            fig.update_yaxes(range=[0, max_y * 1.1])

        current_queue_text = f"{queue}" if queue else "No queue selected"

        return fig, current_queue_text

    # Update dropdown from URL
    @app.callback(
        Output("queue-dropdown", "value"),
        Output("date-picker-single", "date"),
        Output("start-time", "value"),
        Input("url", "search"),
        State("queue-dropdown", "value"),
        prevent_initial_call=True,
    )
    def update_dropdown_from_url(search, current_queue):
        params = urllib.parse.parse_qs(search.lstrip("?"))
        start_dt = datetime.utcnow() - timedelta(hours=24)
        return (
            params.get("queue", [current_queue])[0],
            params.get("date", [start_dt.date().isoformat()])[0],
            params.get("start_time", [start_dt.strftime("%H:%M")])[0],
        )

    # Update URL from dropdown
    @app.callback(
        Output("url", "search", allow_duplicate=True),
        Input("queue-dropdown", "value"),
        Input("date-picker-single", "date"),
        Input("start-time", "value"),
        prevent_initial_call=True,
    )
    def update_url_from_dropdown(value, date, start_time):
        return f"?queue={value}&date={date}&start_time={start_time}"

    @app.callback(
        Output("date-picker-single", "date", allow_duplicate=True),
        Output("start-time", "value", allow_duplicate=True),
        Input("time-range-preset", "value"),
        prevent_initial_call=True,
    )
    def apply_time_preset(preset):
        now = datetime.utcnow()
        durations = {"6h": 6, "12h": 12, "24h": 24, "3d": 72, "7d": 168}
        hours = durations.get(preset, 24)
        start = now - timedelta(hours=hours)
        return start.date().isoformat(), start.strftime("%H:%M")

    @app.callback(
        Output("interval", "interval"),
        Output("interval", "disabled"),
        Input("refresh-interval-dropdown", "value"),
    )
    def update_refresh_interval(value):
        if not value:
            return 86_400_000, True
        return value, False

    @app.callback(
        Output("workspace-store", "data"),
        Output("workspace-display", "children"),
        Input("queue-dropdown", "value"),
    )
    def resolve_workspace(queue):
        if not queue:
            return "", "—"
        try:
            query = f"""
            AppTraces
            | where Properties.queue == "{queue}"
            | where isnotempty(Properties.azureml_workspace_name)
            | take 1
            | project tostring(Properties.azureml_workspace_name)
            """
            rows = run_query(query)
            if rows and rows[0]:
                ws = rows[0][0]
                return ws, ws
        except Exception as e:
            LOG.warning(f"Failed to resolve workspace for queue {queue}: {e}")
        return "", "—"
