import json

from dash import Dash, html, dcc, Input, Output
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from sfs_analysis.sfs_utils import get_site2size, get_sample_site_list, get_theoretical_sfs
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
            if options.dataset_name == 'hgdp':
                site, other_site = 'Adygei', 'Balochi'
            elif options.dataset_name == 'arabidopsis':
                site, other_site = 'Afghanistan', 'Armenia'
            else:
                raise NotImplementedError
        if site == other_site:
            return current_fig
        paths_helper = get_paths_helper(options.dataset_name)
        paths_helper.sfs_dir_chr = paths_helper.sfs_dir + f'chr{options.chr_num}/'
        sfs = np.load(f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}-hst.npy')[1:]
        sfs[-1] *= 2
        site2size = get_site2size(paths_helper)
        hotspot = 2 * min(site2size[site], site2size[other_site])
        num_of_snps = np.sum(sfs)
        theoretical_res = get_theoretical_sfs(num_of_snps, sfs.size)
        current_fig = go.Figure(data=[go.Scatter(x=np.arange(1, sfs.size + 1), y=theoretical_res, line=dict(color='grey', dash='dash'),
                                                 showlegend=False, mode='lines'),
                                      go.Scatter(x=np.arange(1, sfs.size + 1), y=sfs, line=dict(color='blue'),
                                                 mode='lines+markers', showlegend=False),
                                      go.Scatter(x=[hotspot], y=[sfs[hotspot - 1]], line=dict(color='orange'),
                                                 showlegend=False)])
        if hoverData:
            score = hoverData["points"][0]["z"]
        else:
            score = '----'
        current_fig.update_layout(
            title=f'{site} ({site2size[site]}) & {other_site} ({site2size[other_site]}) - Score {score}',
            xaxis_title="Minor Allele Count",
            yaxis_title="Number of SNPs")
        return current_fig

    @app.callback(
        Output('heatmap', 'figure'),
        Input('comparison_method', 'value'))
    def update_graph(comparison_method):
        paths_helper = get_paths_helper(options.dataset_name)
        paths_helper.sfs_dir_chr = paths_helper.sfs_dir + f'chr{options.chr_num}/'
        heatmap_dir_path = f'{paths_helper.sfs_dir_chr}summary/'
        heatmap_path = None
        if comparison_method == 'Theoretical comparison':
            heatmap_path = heatmap_dir_path + 'theoretical_heat.csv'
        if comparison_method == 'Relative comparison':
            heatmap_path = heatmap_dir_path + 'relative_heat.csv'
        df = pd.read_csv(heatmap_path)
        heatmap_np = pd.DataFrame.to_numpy(df)[:, 1:]
        sites_list = df['sites']
        np.fill_diagonal(heatmap_np, np.nan)
        heatmap_fig = px.imshow(heatmap_np, x=sites_list, y=sites_list, text_auto=True,
                                color_continuous_scale='RdBu_r', origin='lower', aspect='auto', width=650, height=650)
        heatmap_fig.update_coloraxes(showscale=False)
        heatmap_fig.update_layout(title=f'Populations HeatMap by {comparison_method} - {options.dataset_name}', title_x=.5)

        return heatmap_fig


def init(options):
    paths_helper = get_paths_helper(options.dataset_name)
    paths_helper.sfs_dir_chr = paths_helper.sfs_dir + f'chr{options.chr_num}/'
    df = pd.read_csv(f'{paths_helper.sfs_dir_chr}summary/theoretical_heat.csv')
    heatmap_np = pd.DataFrame.to_numpy(df)[:, 1:]
    np.fill_diagonal(heatmap_np, np.nan)
    sites_list = get_sample_site_list(options, paths_helper)

    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = Dash(__name__, external_stylesheets=external_stylesheets)
    heatmap_fig = px.imshow(heatmap_np, x=sites_list, y=sites_list, text_auto=True,
                       color_continuous_scale='RdBu_r', origin='lower', aspect='auto', width=650, height=650)
    heatmap_fig.update_coloraxes(showscale=False)
    heatmap_fig.update_layout(title=f'Populations HeatMap - {options.dataset_name}', title_x=.5)
    app.layout = html.Div([
        html.Div([
            html.Div([
            dcc.Dropdown(
                ['Theoretical comparison', 'Relative comparison'],
                'Theoretical comparison',
                id='comparison_method',
            )], style={'width': '49%', 'display': 'inline-block'})]),
        html.Div([
            dcc.Graph(
                id='heatmap',
                figure=heatmap_fig)
        ], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(id='sfs'),
        ], style={'display': 'inline-block', 'width': '49%', 'marginBottom': 250}),
    ])
    callbacks(app, options)
    return app


if __name__ == '__main__':
    options = args_parser()
    app = init(options)
    app.run_server(debug=True, port=options.port)
