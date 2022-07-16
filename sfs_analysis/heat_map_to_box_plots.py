import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from utils.common import args_parser, get_paths_helper


def csv_2_box_plot(heatmap_csv, pop2continent):
    data = pd.read_csv(heatmap_csv)
    continents = set(pop2continent.values())
    plot_data = {c: [] for c in continents}
    for c in continents:
        pops = [k for k, v in pop2continent.items() if v == c]
        for p1, p2 in itertools.combinations(pops, 2):
            plot_data[c].append(data[p1, p2])
    fig1, ax1 = plt.subplots()
    ax1.set_title('Basic Plot')
    ax1.boxplot(plot_data)
    plt.show()


def build_pop2continent(csv_file):
    data = pd.read_csv(csv_file)
    pop2continent = {}
    for row in data.iterrows():
        pop = row.population
        continent = row.region
        if pop in pop2continent:
            assert pop2continent[pop] == continent
        else:
            pop2continent[pop] = continent
    return pop2continent

if __name__ == '__main__':
    arguments = args_parser()
    paths_helper = get_paths_helper(arguments.dataset_name)
    pop2continent = build_pop2continent(arguments.args[0])
    csv_2_box_plot(arguments.args[1], pop2continent)