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
import os

from utils import parse_xml, ts_type
from layout_utils import update_figure_layout

external_stylesheets = [
  "https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css",
]

app = DashProxy(__name__, transforms=[ServersideOutputTransform()], external_stylesheets=external_stylesheets)

server = app.server

parquet_path = "s3://bucketeer-50917d91-a8dc-4c58-9970-d298294a62ca/carter_export.parquet"

if os.path.exists('data/default.parquet'):
    df = dd.read_parquet('data/default.parquet')
else:
    df = dd.read_parquet(parquet_path)
    df.to_parquet('data/default.parquet')

def valid_types(_df=df):
    return [_type for _type in _df["type"].unique().compute() if "<NA>" not in _type]


app.layout = html.Div([
    dcc.Store(id='theme-store', data='dark'),

    html.Div([
        html.Button(
            id='theme-toggle-button',
            children=[
                html.I(className='fas fa-moon'),
                html.Span("Toggle Theme", style={'marginLeft': '8px'})
            ],
            className='theme-toggle-button'
        )
    ], className='theme-toggle-container'),

    html.Div([
        html.H1("Team 204 - Automated Health Insights from Wearable Devices", className='text-center my-4'),
        html.P(
            "By default we display metrics based on Carter's Apple Watch data. Note that he switched to the Apple Watch Ultra in April 2023, so metrics may be measured slightly differently before and after this date. You can upload your own data to visualize your own metrics.",
            className='text-center'
        )
    ], className='centered-section'),

    html.Div([
        dcc.Upload(
            id='upload-data',
            children=html.Div([
                html.A('Upload Export.xml', className='btn-primary')
            ]),
            accept='.xml',
            className='upload-container'
        )
    ], className='centered-section my-3'),

    html.Br(),
    html.Div([
    html.P("Select the metrics you want to visualize:", className='text-center'),
        dcc.Dropdown(
        id='type-dropdown',
        options=[{'label': _type, 'value': _type} for _type in valid_types()],
        value=[],
        className='dccDropdown',
        multi=True,),
    ], className='metrics-section'),
    dcc.Loading(
        id="loading-metrics",
        type="graph",
        fullscreen=True,
        children=html.Div(id='metric-graphs', className='container'),
    ),
    dcc.Loading(dcc.Store(id='df-store'), fullscreen=True, type='cube'),
], id='main-container', className='dark')

@app.callback(
    Output('type-dropdown', 'options'),
    Input('df-store', 'data'),
    prevent_initial_call=True,
)
def update_type_dropdown(_df):
    return [{'label': _type, 'value': _type} for _type in valid_types(_df)]

@app.callback(
    Output('theme-store', 'data'),
    Output('theme-toggle-button', 'children'),
    Output('main-container', 'className'),
    Input('theme-toggle-button', 'n_clicks'),
    State('theme-store', 'data'),
)
def toggle_theme(n_clicks, current_theme):
    if n_clicks:
        new_theme = 'light' if current_theme == 'dark' else 'dark'
    else:
        new_theme = current_theme

    if new_theme == 'dark':
        button_children = [
            html.I(className='fas fa-moon'),
            html.Span(" Light", style={'marginLeft': '8px'})
        ]
    else:
        button_children = [
            html.I(className='fas fa-sun'),
            html.Span(" Dark ", style={'marginLeft': '8px'})
        ]

    return new_theme, button_children, new_theme

@app.callback(
    Output('df-store', 'data'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
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
        _df = df
    logging.debug(f"Updated df:\n{_df.head()}")
    return Serverside(_df)


@app.callback(
    Output("metric-graphs", "children"),
    Input("df-store", "data"),
    Input("type-dropdown", "value"),
    Input("theme-store", "data"),
    prevent_initial_call=True,
)
def update_metric_graphs(_df, types, theme):
    figs = []
    for _type in types:
        if _type:
            series = ts_type(_df, _type)
            fig = px.line(series, title=series.name)
            fig = update_figure_layout(fig, series.name, theme)
            figs.append(fig)
    return html.Div(
        [
            html.Div(
                dcc.Graph(figure=fig),
                className='graph-item'
            ) for fig in figs
        ],
        className='graph-container'
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    app.run(debug=True)