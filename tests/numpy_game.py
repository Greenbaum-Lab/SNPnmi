import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from sfs_analysis.sfs_utils import get_ticks_locations


def heatmap_plot(data_matrix, x_label, y_label, x_ticks, y_ticks, xticks_labels, yticks_labels, c_bar_label,
                 title):
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.title(title, fontsize=18)
    ttl = ax.title
    ttl.set_position([0.5, 1.02])
    s = sns.heatmap(data_matrix, cmap='RdYlGn', ax=ax,
                    cbar_kws={"ticks": np.arange(int(np.nanmax(data_matrix))) + 1, "label": c_bar_label})
    s.set_xlabel(x_label, fontsize=18)
    s.set_ylabel(y_label, fontsize=18)
    s.figure.axes[-1].yaxis.label.set_size(20)
    s.figure.axes[-1].tick_params(labelsize=14)
    plt.xticks(x_ticks, xticks_labels, fontsize=14)
    plt.yticks(y_ticks, yticks_labels, fontsize=14)
    plt.title(title, fontsize=24)
    plt.show()

M_RATES = (np.arange(100) + 1) / (10 ** 6)
GENERATIONS = np.linspace(1, 401, 41).astype(int)

data = np.arange(4000).reshape(100, 40)
y_labels = np.array([1e-6] + [i*1e-5 for i in range(1, 11, 2)])
x_labels = np.array([1, 100, 200, 300, 400])
x_ticks=get_ticks_locations(GENERATIONS, x_labels),
y_ticks=get_ticks_locations(M_RATES, y_labels),

heatmap_plot(data, 'x' ,'y', x_ticks, y_ticks, x_labels, y_labels, 'c', 'hi')