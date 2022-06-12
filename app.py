import json

from dash import Dash, html, dcc, Input, Output
import numpy as np
import pandas as pd
import plotly.express as px

from utils.common import args_parser, get_paths_helper


def callbacks(app, options):
    @app.callback(
        Output('sfs', 'figure'),
        Input('heatmap', 'hoverData'))
    def update_x_timeseries(hoverData):
        if hoverData:
            site, other_site = sorted([hoverData['points'][0]['x'], hoverData['points'][0]['y']])
            paths_helper = get_paths_helper(options.dataset_name)
            with open(f'{paths_helper.sfs_dir}summary/all_hists.json', "rb") as f:
                js = json.load(f)
            sfs = js[f'{site}-{other_site}'][1:]
            df = px.data.gapminder().query(f"country=='{hoverData['points'][0]['x']}'")

            fig = px.line(df, x="year", y="lifeExp", title='Life expectancy in Canada')
        else:
            df = 1
            fig = px.line(df, x="year", y="lifeExp", title='Life expectancy in Canada')
        return fig


def init(options):
    paths_helper = get_paths_helper(options.dataset_name)
    heatmap_np = np.load(f'{paths_helper.sfs_dir}summary/heatmap.npy')
    sites_list = get_site_list()
    heat_df = pd.DataFrame(data=heatmap_np, index=[f'name_{i}' for i in range(5)],
                           columns=[f'name_{i}' for i in range(5)])

    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = Dash(__name__, external_stylesheets=external_stylesheets)

    app.layout = html.Div([
        html.Div([
            dcc.Graph(
                id='heatmap',
                # hoverData={'points': [{'customdata': 'Japan'}]},
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