#!/usr/bin/python3

import os
import sys
from string import ascii_uppercase
from io import BytesIO
from os.path import dirname, abspath
import json

import cairosvg as cairosvg
from PIL import Image
import subprocess

import matplotlib.pyplot as plt
import msprime


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Timer, Loader
from utils.common import get_paths_helper, get_dataset_vcf_files_names

SCRIPT_PATH = os.path.abspath(__file__)
SIMULAITION_NAME = 'sim_dip_v1'
POPULATION_SIZE = 2000
NUMBER_OF_SUBPOPS = 2
OUTPUT_SIZE = 1000
INDV_PER_POP = POPULATION_SIZE // NUMBER_OF_SUBPOPS
POP_SAMPLE_SIZE = OUTPUT_SIZE / NUMBER_OF_SUBPOPS
def run_simulation():
    demography = msprime.Demography()
    for i in range(NUMBER_OF_SUBPOPS):
        demography.add_population(name=ascii_uppercase[i], initial_size=INDV_PER_POP)
    demography.add_population(name="AB", initial_size=POPULATION_SIZE)
    demography.add_population_split(time=5000, derived=[e for e in ascii_uppercase[:NUMBER_OF_SUBPOPS]], ancestral="AB")

    ts = msprime.sim_ancestry(samples={ascii_uppercase[i]: POP_SAMPLE_SIZE for i in range(NUMBER_OF_SUBPOPS)}, sequence_length=5e8, demography=demography,
                              recombination_rate=1e-8, random_seed=1)
    mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=5e-7, random_seed=1)
    return mts

def run_simulation_and_save_vcf(paths_helper):
    with Loader("Running simulation"):
        mts = run_simulation()
    os.makedirs(paths_helper.data_dir, exist_ok=True)
    with Loader("Saving VCF"):
        with open(paths_helper.data_dir + SIMULAITION_NAME + '.vcf', 'w+') as f:
            mts.write_vcf(f)


def plot_tree(ts):
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
        f.write('A\n' * OUTPUT_SIZE)

def add_simulation_to_data_config_file(paths_helper):
    data_json = '../config/config.data.json'
    with open(data_json, "r") as f:
        js = json.load(f)
    assert SIMULAITION_NAME not in js, "Cannot overwrite simulations! First delete old simulation"
    js[SIMULAITION_NAME] = {'vcf_files_name': [f'{SIMULAITION_NAME}.vcf'],
                            'vcf_files_short_names': ['chr1'],
                            'num_chrs': 1,
                            'num_individuals': OUTPUT_SIZE,
                            'indlist_file_name': 'inlist.txt',
                            'sample_sites_file_name': 'sampleSite.txt'}
    with open(data_json, "w") as f:
        json.dump(js, f)


if __name__ == '__main__':
    paths_helper = get_paths_helper(SIMULAITION_NAME)
    add_simulation_to_data_config_file(paths_helper)
    run_simulation_and_save_vcf(paths_helper)
    copy_runner_to_vcf_dir(paths_helper)
    write_ind_list_for_ns(paths_helper)
