import plotly.express as px
import pandas as pd

from dash import Dash, html, dcc, Input, Output, State, callback, dash_table
import io
import base64

from utils import parse_xml

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
        df = pd.read_csv('data/carter_export_2024.csv')
    else:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        try:
            df = parse_xml(io.StringIO(decoded.decode('utf-8')))
        except Exception as e:
            return str(e)
    output = dash_table.DataTable(df.to_dict('records'), [{'name': i, 'id': i} for i in df.columns], page_size=20)
    return output

if __name__ == '__main__':
    app.run(debug=True)