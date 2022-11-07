import itertools
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def csv_2_box_plot(heatmap_csv, pop2continent, output_dir):
    data = pd.read_csv(heatmap_csv)
    continents = set(pop2continent.values())
    two_d_continent_vals = {(c1, c2): [] for c1, c2 in itertools.combinations_with_replacement(continents, 2)}
    for c1, c2 in itertools.combinations_with_replacement(continents, 2):
        c1_pops = [k for k, v in pop2continent.items() if v == c1]
        c2_pops = [k for k, v in pop2continent.items() if v == c2]
        for p1 in c1_pops:
            for p2 in c2_pops:
                val = float(data.loc[data["sites"] == p1][p2])
                if val > 0:
                    two_d_continent_vals[(c1, c2)].append(val)
    for c in continents:
        fig1, ax1 = plt.subplots()
        c_vals = {}
        for (c1, c2), v in two_d_continent_vals.items():
            if c1 == c:
                c_vals[c2] = v
            elif c2 == c:
                c_vals[c1] = v
        ax1.set_title(f'{c}')
        ax1.boxplot(c_vals.values())
        ax1.set_xticklabels(c_vals.keys(), fontsize=6)
        plt.savefig(f"{output_dir}/{c}.svg")
        plt.clf()


def build_pop2continent(csv_file):
    data = pd.read_csv(csv_file, sep='\t')
    pop2continent = {}
    for _, row in data.iterrows():
        pop = row.population
        continent = row.region
        if pop in pop2continent:
            assert pop2continent[pop] == continent
        else:
            pop2continent[pop] = continent
    return pop2continent


if __name__ == '__main__':
    # arguments = args_parser()
    # paths_helper = get_paths_helper(arguments.dataset_name)
    pop2continent = build_pop2continent(sys.argv[1])
    csv_2_box_plot(sys.argv[2], pop2continent, sys.argv[3])