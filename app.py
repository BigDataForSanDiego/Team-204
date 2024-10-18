import dask.dataframe as dd
import plotly.express as px

from dash_extensions.enrich import (
    DashProxy,
    html,
    dcc,
    Input,
    Output,
    State,
    Serverside,
    ServersideOutputTransform,
)

import io
import base64
import logging

from utils import parse_xml, ts_type


app = DashProxy(__name__, transforms=[ServersideOutputTransform()])

server = app.server


def valid_types(_df=dd.read_parquet("data/carter_export.parquet")):
    return [_type for _type in _df["type"].unique().compute() if "nan" not in _type]


app.layout = [
    html.H1(
        "Team 204 - Automated Health Insights from Wearable Devices",
        style={"textAlign": "center"},
    ),
    html.P(
        "By default we display metrics based on Carter's Apple Watch data. Note that he switched to the Apple Watch Ultra in April 2023, so metrics may be measured slightly differently before and after this date.You can upload your own data to visualize your own metrics."
    ),
    dcc.Upload(
        id="upload-data",
        children=html.Div([html.A("Upload Export.xml")]),
        accept=".xml",
        style={
            "width": "240px",
            "height": "60px",
            "lineHeight": "60px",
            "borderWidth": "1px",
            "borderStyle": "dashed",
            "borderRadius": "5px",
            "textAlign": "center",
            "margin": "auto",
        },
    ),
    html.Br(),
    html.P("Select the metrics you want to visualize:"),
    dcc.Dropdown(
        id="type-dropdown",
        options=valid_types(),
        value=[],
        multi=True,
    ),
    dcc.Loading(
        id="loading-metrics",
        type="graph",
        fullscreen=True,
        children=html.Div(id="metric-graphs"),
    ),
    dcc.Loading(dcc.Store(id="df-store"), fullscreen=True, type="cube"),
]


@app.callback(
    Output("type-dropdown", "options"),
    Input("df-store", "data"),
    prevent_initial_call=True,
)
def valid_types_callback(_df):
    return valid_types(_df)


@app.callback(
    Output("df-store", "data"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
)
def update_df(contents, filename):
    if contents:
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        try:
            logging.info(f"Parsing XML file: {filename}")
            _df = parse_xml(io.StringIO(decoded.decode("utf-8")))
        except Exception as e:
            logging.error(e)
    else:
        _df = dd.read_parquet("data/carter_export.parquet")
    logging.debug(f"Updated df:\n{_df.head()}")
    return Serverside(_df)


@app.callback(
    Output("metric-graphs", "children"),
    Input("df-store", "data"),
    Input("type-dropdown", "value"),
    prevent_initial_call=True,
)
def update_metric_graphs(_df, types):
    figs = []
    for col in types:
        series = ts_type(_df, col)
        fig = px.line(series, title=series.name)
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=series.name,
            showlegend=False,
        )
        figs.append(fig)
    return [dcc.Graph(figure=fig) for fig in figs]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
