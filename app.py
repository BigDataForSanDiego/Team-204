import plotly.express as px
import plotly.graph_objects as go

import pandas as pd

from dash import Dash, html, dcc, Input, Output, State, callback, dash_table
import io
import base64

from utils import parse_xml, parse_csv, ts_type

app = Dash(__name__)

app.layout = [
    html.H1("Team 204 - Automated Health Insights from Wearable Devices", style={'textAlign': 'center'}),
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
    html.Div(id='output-data-upload'),
]

@callback(
    Output('output-data-upload', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
)
def update_output(contents, filename):
    if contents is None:
        df = parse_csv('data/carter_export_2024_jan.csv')
    else:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        try:
            df = parse_xml(io.StringIO(decoded.decode('utf-8')))
        except Exception as e:
            return str(e)
    # output = dash_table.DataTable(df.to_dict('records'), [{'name': i, 'id': i} for i in df.columns], page_size=20)
    cols = ['HKQuantityTypeIdentifierActiveEnergyBurned', 'HKQuantityTypeIdentifierTimeInDaylight']
    figs = []
    for col in cols:
        series = ts_type(df, col)
        fig = px.line(series, title=series.name)
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=series.name,
            showlegend=False,
        )
        figs.append(fig)
    return [to_dash_component(fig) for fig in figs]


def to_dash_component(var):
    if isinstance(var, pd.DataFrame):
        return dash_table.DataTable(var.to_dict('records'), [{'name': i, 'id': i} for i in var.columns])
    elif isinstance(var, pd.Series):
        return dcc.Graph(figure=px.line(var, x=var.index, y=var.values))
    elif isinstance(var, go.Figure):
        return dcc.Graph(figure=var)
    else:
        raise ValueError(f'Invalid variable type: {type(var)}')


if __name__ == '__main__':
    app.run(debug=True)