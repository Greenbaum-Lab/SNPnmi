#!/usr/bin/python3

import os
import sys
from io import BytesIO
from os.path import dirname, abspath

from PIL import Image
import subprocess

import matplotlib.pyplot as plt
import cairosvg
import msprime

from utils.common import get_paths_helper

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

SCRIPT_NAME = os.path.basename(__file__)
SIMULAITION_NAME = 'sim_2_v0_coal'


def run_simulation(paths_helper):
    pop_configs = [msprime.PopulationConfiguration(sample_size=100),
                   msprime.PopulationConfiguration(sample_size=100)]

    ts = msprime.simulate(population_configurations=pop_configs, length=1e6, Ne=2000, mutation_rate=1e-6,
                          recombination_rate=1e-8,
                          demographic_events=[
                              msprime.MassMigration(10000, source=1, dest=0, proportion=1)
                          ])
    os.makedirs(paths_helper.data_dir, exist_ok=True)
    with open(paths_helper.data_dir + SCRIPT_NAME + '.vcf', 'w+') as f:
        ts.write_vcf(f)


def plot_tree(ts):
    color_map = {0: 'red', 1: 'blue', 2: 'green'}
    tree = ts.first()
    node_colors = {u: color_map[tree.population(u)] for u in tree.nodes()}
    img = cairosvg.svg2png(tree.draw(node_colours=node_colors))
    img = Image.open(BytesIO(img))
    plt.imshow(img)
    plt.show()


def copy_runner_to_vcf_dir(paths_helper):
    subprocess.Popen(['cp', SCRIPT_NAME, paths_helper.data_dir])


if __name__ == '__main__':
    paths_helper = get_paths_helper(SIMULAITION_NAME)
    run_simulation(paths_helper)
    copy_runner_to_vcf_dir(paths_helper)
