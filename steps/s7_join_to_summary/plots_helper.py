#!/usr/bin/env python

import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import repr_num
import seaborn as sns

class PlotConsts:
    line_width = 1.5
    scatter_size = 4

    @staticmethod
    def get_class_names(options, mac_maf):
        mac_class_names = np.arange(options.mac[0], options.mac[1] + 1) if options.dataset_name != 'arabidopsis' else \
            np.arange(options.mac[0], options.mac[1] + 1, 2)
        maf_class_names = np.arange(options.maf[0], options.maf[1] + 1) / 100
        class_names = mac_class_names if mac_maf == 'mac' else maf_class_names

        return class_names


def r2score(true, pred):
    true = true.reshape((-1, 1))
    pred = pred.reshape((-1, 1))
    numerator = ((true - pred) ** 2).sum(axis=0, dtype=np.float64)
    denominator = ((true - np.average(true, axis=0)) ** 2).sum(axis=0, dtype=np.float64)
    nonzero_denominator = denominator != 0
    nonzero_numerator = numerator != 0
    valid_score = nonzero_denominator & nonzero_numerator
    output_scores = np.ones([true.shape[1]])
    output_scores[valid_score] = 1 - (numerator[valid_score] / denominator[valid_score])
    output_scores[nonzero_numerator & ~nonzero_denominator] = 0.0
    return np.average(output_scores)


def plot_per_class(options, mac_maf, values, std, scats, polynomials, base_lines, colors, labels, title, output, log_scale=False,
                   y_label="", legend_title=""):
    f = plt.figure()
    f.set_figwidth(8)
    f.set_figheight(6)
    class_names = PlotConsts.get_class_names(options, mac_maf)
    num_of_plots = values.shape[0] if values is not None else 0
    num_of_stds = std.shape[0] if std is not None else 0
    num_of_scats = scats.shape[0] if scats is not None else 0
    num_of_base_lines = base_lines.size if base_lines is not None else 0

    for idx in range(num_of_plots):
        plt.plot(class_names, values[idx], color=colors[idx], label=labels[idx])
        if idx < num_of_stds:
            plt.fill_between(class_names, y1=values[idx] - std[idx], y2=values[idx] + std[idx],
                             alpha=0.3, color=colors[idx])

    for idx in range(num_of_scats):
        plt.scatter(class_names, scats[idx], color=colors[num_of_plots + idx],
                    label=labels[num_of_plots + idx], s=PlotConsts.scatter_size)

    for idx in range(num_of_scats):
        p = np.poly1d(polynomials[idx])
        y_hat = p(class_names)
        plt.plot(class_names, y_hat, color=colors[num_of_plots + idx], linestyle='--',
                 linewidth=PlotConsts.line_width)
        e = [f'{repr_num(polynomials[idx, i])}' if i == 0 or polynomials[
            idx, i] < 0 else f'+{repr_num(polynomials[idx, i])}' for i in range(len(polynomials[idx]))]
        equation = [f'{e[i]} x^{len(e) - (i + 1)}' if len(e) - (i + 1) > 1 else f'{e[i]}' if len(e) - (
                i + 1) == 0 else f'{e[i]} x' for i in range(len(e))]
        text = f"$y={''.join(equation)}$\n$R^2 = {repr_num(r2score(scats[idx], y_hat))}$"
        plt.gca().text(.01 + .02 * idx, .99, text, transform=plt.gca().transAxes,
                       fontsize=10, verticalalignment='top')
        plt.plot(0.05, 0.95, transform=plt.gca().transAxes, color='none')

    for idx in range(num_of_base_lines):
        plt.plot([min(class_names), max(class_names)], [base_lines[idx] for _ in range(2)],
                 linestyle='--', color=colors[num_of_plots + num_of_scats + idx], alpha=0.8,
                 linewidth=PlotConsts.line_width)

    if log_scale:
        plt.yscale('log')
    plt.xlabel(f"AFC({mac_maf.upper()})", fontsize=18)
    if y_label:
        plt.ylabel(y_label, fontsize=18)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.title(title, fontsize=24)
    if legend_title:
        legend = plt.legend(title=legend_title, loc='best', fontsize=12)
        plt.setp(legend.get_title(), fontsize=12)
    plt.savefig(output)
    plt.close()


def heatmap_plot(output, data_matrix, x_label, y_label, x_ticks, y_ticks, c_bar_label, title, y_bins=False,
                 x_bins=False):
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.title(title, fontsize=18)
    ttl = ax.title
    ttl.set_position([0.5, 1.02])
    # ax.set_xticks(x_ticks)
    s = sns.heatmap(data_matrix, cmap='RdYlGn', ax=ax, xticklabels=x_ticks, yticklabels=y_ticks,
                    cbar_kws={"ticks": np.arange(int(np.nanmax(data_matrix))) + 1, "label": c_bar_label})
    if x_bins:
        plt.locator_params(axis='x', nbins=x_bins)
    if y_bins:
        plt.locator_params(axis='y', nbins=y_bins)
    s.set_xlabel(x_label, fontsize=18)
    s.set_ylabel(y_label, fontsize=18)
    s.figure.axes[-1].yaxis.label.set_size(20)
    s.figure.axes[-1].tick_params(labelsize=14)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.title(title, fontsize=24)
    plt.show()
    plt.savefig(output)
