import plotly.express as px
import plotly.graph_objects as go

import pandas as pd
import dask.dataframe as dd

from dash import Dash, html, dcc, Input, Output, State, callback, dash_table
import io
import base64

from utils import parse_xml, ts_type

import logging

app = Dash(__name__)

df = dd.read_parquet('data/carter_export.parquet')

server = app.server

valid_types = [_type for _type in df['type'].unique().compute() if 'nan' not in _type]

app.layout = [
    html.H1("Team 204 - Automated Health Insights from Wearable Devices", style={'textAlign': 'center'}),
    html.P("By default we display metrics based on Carter's Apple Watch data. Note that he switched to the Apple Watch Ultra in April 2023, so metrics may be measured slightly differently before and after this date.You can upload your own data to visualize your own metrics."),
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            html.A('Upload Export.xml')
        ]),
        accept='.xml',
        style={
            'width': '240px',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': 'auto'
        },
    ),
    html.Br(),
    html.P("Select the metrics you want to visualize:"),
    dcc.Dropdown(
        id='type-dropdown',
        options=[{'label': _type, 'value': _type} for _type in valid_types],
        value=['Distance Walking Running (mi)'],
        multi=True,
    ),
    html.Div(id='output-data-upload'),
]

@callback(
    Output('output-data-upload', 'children'),
    Input('type-dropdown', 'value'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
)
def update_output(types, contents, filename):
    global df
    if contents:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        try:
            logging.info(f"Parsing XML file: {filename}")
            df = parse_xml(io.StringIO(decoded.decode('utf-8')))
        except Exception as e:
            logging.error(e)
    figs = []
    for col in types:
        series = ts_type(df, col)
        fig = px.line(series, title=series.name)
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=series.name,
            showlegend=False,
        )
        figs.append(fig)
    return [dcc.Graph(figure=fig) for fig in figs]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)