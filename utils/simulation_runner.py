#!/usr/bin/python3

import os
import sys
from io import BytesIO
from os.path import dirname, abspath

import cairosvg as cairosvg
from PIL import Image
import subprocess

import matplotlib.pyplot as plt
import msprime


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper

SCRIPT_PATH = os.path.abspath(__file__)
SIMULAITION_NAME = 'sim_2_v0_coal'
POPULATION_SIZE = 1000


def run_simulation():
    pop_configs = [msprime.PopulationConfiguration(sample_size=500),
                   msprime.PopulationConfiguration(sample_size=500)]

    ts = msprime.simulate(population_configurations=pop_configs, length=1e8, Ne=2000, mutation_rate=1e-7,
                          recombination_rate=1e-8,
                          demographic_events=[
                              msprime.MassMigration(10000, source=1, dest=0, proportion=1)
                          ])
    return ts


def run_simulation_and_save_vcf(paths_helper):
    with Timer("Running simulation"):
        ts = run_simulation()
    os.makedirs(paths_helper.data_dir, exist_ok=True)
    with Timer("Saving VCF"):
        with open(paths_helper.data_dir + SIMULAITION_NAME + '.vcf', 'w+') as f:
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
    print(f"Copy {SCRIPT_PATH} to {paths_helper.data_dir}")
    subprocess.Popen(['cp', SCRIPT_PATH, paths_helper.data_dir])


def write_ind_list_for_ns(paths_helper):
    with open(paths_helper.data_dir + 'sampleSite.txt', 'w+') as f:
        f.write('A')
    with open(paths_helper.data_dir + 'inlist.txt', 'w+') as f:
        f.write('A\n' * POPULATION_SIZE)


if __name__ == '__main__':
    paths_helper = get_paths_helper(SIMULAITION_NAME)
    run_simulation_and_save_vcf(paths_helper)
    copy_runner_to_vcf_dir(paths_helper)
    write_ind_list_for_ns(paths_helper)
