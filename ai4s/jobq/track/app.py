# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import webbrowser
from threading import Timer

import dash_bootstrap_components as dbc
from dash import Dash, html

from .components import (
    active_workers,
    cpu_utilization,
    errors,
    preemption_events,
    queue_size,
    ram_utilization,
    task_runtimes,
    tasks_completed,
    tasks_starting,
)


def run_with_default_queue(queue_name=None, debug=False, port=8050):
    debug = debug or False
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        external_scripts=[dbc._js_dist[0]["relative_package_path"]],
    )
    app.title = "JobQ Track"

    app.layout = html.Div(
        [
            html.H1("JobQ Track"),
            active_workers.layout(default_queue=queue_name),
        ]
    )

    active_workers.register_callbacks(app)
    queue_size.register_callbacks(app)
    tasks_starting.register_callbacks(app)
    tasks_completed.register_callbacks(app)
    task_runtimes.register_callbacks(app)
    cpu_utilization.register_callbacks(app)
    ram_utilization.register_callbacks(app)
    errors.register_callbacks(app)
    preemption_events.register_callbacks(app)

    def open_browser():
        webbrowser.open_new_tab(f"http://127.0.0.1:{port}/")

    Timer(2, open_browser).start()
    app.run(debug=debug, dev_tools_ui=debug, port=port)


if __name__ == "__main__":
    run_with_default_queue()
