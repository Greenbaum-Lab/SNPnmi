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
from utils.common import get_paths_helper, get_dataset_vcf_files_names

SCRIPT_PATH = os.path.abspath(__file__)
SIMULAITION_NAME = 'debug'
POPULATION_SIZE = 2000
NUMBER_OF_SUBPOPS = 2
INDV_PER_POP = POPULATION_SIZE // NUMBER_OF_SUBPOPS

def run_simulation():
    demography = msprime.Demography()
    for i in range(NUMBER_OF_SUBPOPS):
        demography.add_population(name=ascii_uppercase[i], initial_size=INDV_PER_POP)
    demography.add_population(name="AB", initial_size=POPULATION_SIZE)
    demography.add_population_split(time=10000, derived=[e for e in ascii_uppercase[:NUMBER_OF_SUBPOPS]], ancestral="AB")

    ts = msprime.sim_ancestry(samples={'A': 50, 'B': 50}, sequence_length=5e6, demography=demography,
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


def fix_ref_in_vcf_to_be_minor_allele(SIMULAITION_NAME):
    paths_helper = get_paths_helper(SIMULAITION_NAME)
    vcf_file_path = paths_helper.data_dir + SIMULAITION_NAME + '.vcf'
    new_vcf_file_path = paths_helper.data_dir + SIMULAITION_NAME + '_fix.vcf'
    stats_file_path = paths_helper.vcf_stats_folder + SIMULAITION_NAME + '.vcf.freq.frq'
    with open(vcf_file_path, "r") as vcf_file, open(stats_file_path, 'r') as stats_file, open(new_vcf_file_path, 'w') as new_f:
        old_vcf_line = vcf_file.readline()
        while old_vcf_line.startswith('#'):
            if "REF" not in old_vcf_line:
                new_f.write(old_vcf_line)
                old_vcf_line = vcf_file.readline()
                continue
            else:
                stats_line_lst = old_vcf_line.split('\t')
                VCF_POS = [i for i in range(len(stats_line_lst)) if stats_line_lst[i] == "POS"][0]
                VCF_REF = [i for i in range(len(stats_line_lst)) if stats_line_lst[i] == "REF"][0]
                new_f.write(old_vcf_line)
                old_vcf_line = vcf_file.readline()

        first_stats_line = stats_file.readline()
        stats_lst = first_stats_line.split('\t')
        STATS_POS = [i for i in range(len(stats_lst)) if stats_lst[i] == "POS"][0]
        stats_line = stats_file.readline()
        while stats_line:
            stats_line_lst = stats_line.split('\t')
            stats_pos = int(stats_line_lst[STATS_POS])
            old_vcf_line_lst = old_vcf_line.split('\t')
            vcf_pos = int(old_vcf_line_lst[VCF_POS])

            assert vcf_pos == stats_pos
            ref = stats_line_lst[-2].split(":")
            non_ref = stats_line_lst[-1].split(":")
            if float(non_ref[-1]) > float(ref[-1]):
                old_vcf_line_lst[VCF_REF] = str(non_ref[0])
                old_vcf_line = '\t'.join(old_vcf_line_lst)
            new_f.write(old_vcf_line)
            stats_line = stats_file.readline()
            old_vcf_line = vcf_file.readline()
        assert not old_vcf_line
    print("Done!")



if __name__ == '__main__':
    # paths_helper = get_paths_helper(SIMULAITION_NAME)
    # run_simulation_and_save_vcf(paths_helper)
    # copy_runner_to_vcf_dir(paths_helper)
    # write_ind_list_for_ns(paths_helper)
    fix_ref_in_vcf_to_be_minor_allele("debug")
