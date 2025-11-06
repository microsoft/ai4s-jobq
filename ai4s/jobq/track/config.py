# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os

import plotly.graph_objects as go
import plotly.io as pio

solarized_light_template = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Segoe UI, sans-serif", color="#657b83"),
        paper_bgcolor="#fdf6e3",
        plot_bgcolor="#eee8d5",
        xaxis=dict(
            gridcolor="#eee8d5",
            zerolinecolor="#93a1a1",
            linecolor="#586e75",
            tickcolor="#586e75",
        ),
        yaxis=dict(
            gridcolor="#eee8d5",
            zerolinecolor="#93a1a1",
            linecolor="#586e75",
            tickcolor="#586e75",
        ),
        colorway=[
            "#268bd2",
            "#2aa198",
            "#859900",
            "#b58900",
            "#cb4b16",
            "#d33682",
            "#6c71c4",
            "#dc322f",
        ],
    )
)

pio.templates["solarized_light"] = solarized_light_template
pio.templates.default = "solarized_light"


def get_workspace_id():
    return os.environ["JOBQ_LA_WORKSPACE_ID"]
