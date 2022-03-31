#!/usr/bin/python3

import os
import sys
from string import ascii_uppercase
from io import BytesIO
from os.path import dirname, abspath

import cairosvg as cairosvg
from PIL import Image
import subprocess

import matplotlib.pyplot as plt
import msprime


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Timer, Loader
from utils.common import get_paths_helper

SCRIPT_PATH = os.path.abspath(__file__)
SIMULAITION_NAME = 'sim_dip_v0'
POPULATION_SIZE = 1000
NUMBER_OF_SUBPOPS = 2
INDV_PER_POP = POPULATION_SIZE // NUMBER_OF_SUBPOPS

def run_simulation():
    demography = msprime.Demography()
    for i in range(NUMBER_OF_SUBPOPS):
        demography.add_population(name=ascii_uppercase[i], initial_size=INDV_PER_POP)
    demography.add_population(name="AB", initial_size=POPULATION_SIZE)
    demography.add_population_split(time=10000, derived=[e for e in ascii_uppercase[:NUMBER_OF_SUBPOPS]], ancestral="AB")

    ts = msprime.sim_ancestry(samples={'A': 5, 'B': 5}, sequence_length=5e3, demography=demography,
                              recombination_rate=1e-8, random_seed=1)
    mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=5e-5, random_seed=1)
    return mts
# sequence_length=5e8
#  mutation_rate=5e-7
def run_simulation_and_save_vcf(paths_helper):
    with Loader("Running simulation"):
        ts = run_simulation()
    os.makedirs(paths_helper.data_dir, exist_ok=True)
    with Loader("Saving VCF"):
        with open(paths_helper.data_dir + SIMULAITION_NAME + '.vcf', 'w+') as f:
            ts.write_vcf(f)


def plot_tree(ts):
    color_map = {0: 'red', 1: 'blue', 2: 'green'}
    tree = ts.first()
    node_colors = {u: color_map[tree.population(u)] for u in tree.nodes()}
    img = cairosvg.svg2png(ts.draw_svg(y_axis=True))
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

