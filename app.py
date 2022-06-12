import json

from dash import Dash, html, dcc, Input, Output
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from sfs_analysis.sfs_utils import get_site2size
from utils.common import args_parser, get_paths_helper

current_fig = None
def callbacks(app, options):

    @app.callback(
        Output('sfs', 'figure'),
        Input('heatmap', 'hoverData'))
    def update_x_timeseries(hoverData):
        global current_fig
        if hoverData:
            site, other_site = sorted([hoverData['points'][0]['x'], hoverData['points'][0]['y']])
        else:
            site, other_site = 'Adygei', 'Balochi'
        if site == other_site:
            return current_fig
        paths_helper = get_paths_helper(options.dataset_name)
        sfs = np.load(f'{paths_helper.sfs_dir}{site}/{site}-{other_site}-hst.npy')[1:]
        site2size = get_site2size(paths_helper)
        hotspot = 2 * min(site2size[site], site2size[other_site])
        current_fig = go.Figure(data=[go.Scatter(x=np.arange(1, sfs.size + 1), y=sfs, line=dict(color='blue'),
                                                 mode='lines+markers', showlegend=False),
                                      go.Scatter(x=[hotspot], y=[sfs[hotspot - 1]], line=dict(color='orange'),
                                                 showlegend=False)])

        current_fig.update_layout(
            title=f'{site} ({site2size[site]}) & {other_site} ({site2size[other_site]})',
            xaxis_title="Minor Allele Count",
            yaxis_title="Number of SNPs")
        return current_fig


def init(options):
    names = ['Adygei', 'Balochi', 'BantuKenya', 'Burusho', 'Colombian']
    heatmap_np = (np.arange(25).reshape(-5, 5) + 10) / 20
    heat_df = pd.DataFrame(data=heatmap_np, index=names, columns=names)

    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = Dash(__name__, external_stylesheets=external_stylesheets)

    app.layout = html.Div([
        html.Div([
            dcc.Graph(
                id='heatmap',
                figure=px.imshow(heat_df)
            )
        ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
        html.Div([
            dcc.Graph(id='sfs'),
        ], style={'display': 'inline-block', 'width': '49%'}),
    ])
    callbacks(app, options)
    return app


if __name__ == '__main__':
    options = args_parser()
    app = init(options)
    app.run_server(debug=True)