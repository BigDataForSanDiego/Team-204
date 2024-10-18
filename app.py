import plotly.express as px
import plotly.graph_objects as go

import pandas as pd
import dask.dataframe as dd

from dash import Dash, html, dcc, Input, Output, State, callback
import io
import base64

from utils import parse_xml, ts_type
from layout_utils import update_figure_layout  # File to change layouts for graphs

import logging

# Include FontAwesome for icons
external_stylesheets = [
  "https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css",
]

app = Dash(__name__, external_stylesheets=external_stylesheets)

df = dd.read_parquet('data/anihealthdata.parquet')

server = app.server

valid_types = [_type for _type in df['type'].unique().compute() if 'nan' not in _type]

app.layout = html.Div([
    # Store Component to Keep Track of Theme
    dcc.Store(id='theme-store', data='dark'),  # Initialize with 'dark' theme

    # Theme Toggle Button
    html.Div([
        html.Button(
            id='theme-toggle-button',
            children=[
                html.I(className='fas fa-moon'),  # Initial icon for dark mode
                html.Span("Toggle Theme", style={'marginLeft': '8px'})
            ],
            className='theme-toggle-button'
        )
    ], className='theme-toggle-container'),  # Container for the toggle button

    # Header and Description
    html.Div([
        html.H1("Team 204 - Automated Health Insights from Wearable Devices", className='text-center my-4'),
        html.P(
            "By default we display metrics based on Carter's Apple Watch data. Note that he switched to the Apple Watch Ultra in April 2023, so metrics may be measured slightly differently before and after this date. You can upload your own data to visualize your own metrics.",
            className='text-center'
        )
    ], className='centered-section'),

    # Upload Button
    html.Div([
        dcc.Upload(
            id='upload-data',
            children=html.Div([
                html.A('Upload Export.xml', className='btn-primary')
            ]),
            accept='.xml',
            className='upload-container'
        )
    ], className='centered-section my-3'),  # Center the upload button

    html.Br(),

    # Metrics Selection
    html.Div([
        html.P("Select the metrics you want to visualize:", className='text-center'),
        dcc.Dropdown(
            id='type-dropdown',
            options=[{'label': _type, 'value': _type} for _type in valid_types],
            value=['Distance Walking Running (mi)'],
            multi=True,
            className='dccDropdown'
        ),
    ], className='metrics-section'),

    # Output Graphs
    html.Div(id='output-data-upload', className='container'),
], id='main-container', className='dark')  # Default to dark theme

@callback(
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

    # Update the icon based on the new theme
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

@callback(
    Output('output-data-upload', 'children'),
    Input('type-dropdown', 'value'),
    Input('upload-data', 'contents'),
    Input('theme-store', 'data'),  # Updated to use theme-store
    State('upload-data', 'filename'),
)
def update_output(types, contents, selected_theme, filename):
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
        # Use the new function to update the figure layout
        fig = update_figure_layout(fig, series.name, selected_theme)
        figs.append(fig)
    # Wrap each graph in a Div for spacing 
    return html.Div(
        [
            html.Div(
                dcc.Graph(figure=fig),
                className='graph-item'  # Use graph-item for individual graph spacing
            ) for fig in figs
        ],
        className='graph-container'  # Use graph-container for flex layout
    )

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
