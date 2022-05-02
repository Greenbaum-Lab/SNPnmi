#!/usr/bin/python3

import os
import sys
from string import ascii_uppercase
from io import BytesIO
from os.path import dirname, abspath
import json
import cairosvg as cairosvg
import numpy as np
from PIL import Image
import subprocess
import matplotlib.pyplot as plt
import msprime

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Loader
from utils.common import get_paths_helper


class Simulation:
    def __init__(self):
        self.OUTPUT_SIZE = 1000
        self.POPULATION_SIZE = 2000
        self.NUMBER_OF_SUBPOPS = 2
        self.INDV_PER_POP = self.POPULATION_SIZE // self.NUMBER_OF_SUBPOPS
        self.POP_SAMPLE_SIZE = self.OUTPUT_SIZE // self.NUMBER_OF_SUBPOPS

    def run_simulation(self):
        demography = msprime.Demography()
        for i in range(self.NUMBER_OF_SUBPOPS):
            demography.add_population(name=ascii_uppercase[i], initial_size=self.INDV_PER_POP)
        demography.add_population(name="AB", initial_size=self.POPULATION_SIZE)
        demography.add_population_split(time=1000, derived=[e for e in ascii_uppercase[:self.NUMBER_OF_SUBPOPS]], ancestral="AB")

        ts = msprime.sim_ancestry(samples={ascii_uppercase[i]: self.POP_SAMPLE_SIZE for i in range(self.NUMBER_OF_SUBPOPS)}, sequence_length=5e8, demography=demography,
                                  recombination_rate=1e-8, random_seed=1)
        mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=8e-7, random_seed=1)
        return mts

    def run_simulation_and_save_vcf(self, paths_helper, simulation_name):
        with Loader("Running simulation"):
            mts = self.run_simulation()
        os.makedirs(paths_helper.data_dir, exist_ok=True)
        with Loader("Saving VCF"):
            with open(paths_helper.data_dir + simulation_name + '.vcf', 'w') as f:
                mts.write_vcf(f)

    def plot_tree(self, ts):
        img = cairosvg.svg2png(ts.draw_svg(y_axis=True))
        img = Image.open(BytesIO(img))
        plt.imshow(img)
        plt.show()

    def copy_runner_to_vcf_dir(self, paths_helper):
        SCRIPT_PATH = os.path.abspath(__file__)
        print(f"Copy {SCRIPT_PATH} to {paths_helper.data_dir}")
        subprocess.Popen(['cp', SCRIPT_PATH, paths_helper.data_dir])

    def write_ind_list_for_ns(self, paths_helper):
        with open(paths_helper.data_dir + 'sampleSite.txt', 'w+') as f:
            f.write('A')
        with open(paths_helper.data_dir + 'inlist.txt', 'w+') as f:
            f.write('A\n' * self.OUTPUT_SIZE)

    def add_simulation_to_data_config_file(self, paths_helper, simulation_name):
        data_json = paths_helper.repo + 'config/config.data.json'
        with open(data_json, "r") as f:
            js = json.load(f)
        assert simulation_name not in js, "Cannot overwrite simulations! First delete old simulation"
        js[simulation_name] = {'vcf_files_names': [f'{simulation_name}.vcf'],
                               'vcf_files_short_names': ['chr1'],
                               'num_chrs': 1,
                               'num_individuals': self.OUTPUT_SIZE,
                               'indlist_file_name': 'inlist.txt',
                               'sample_sites_file_name': 'sampleSite.txt'}
        with open(data_json, "w") as f:
            json.dump(js, f)

    def write_gt_ns_output(self, paths_helper):
        all_individuals = np.arange(self.OUTPUT_SIZE)
        sub_pops = [all_individuals[i * self.POP_SAMPLE_SIZE: (i + 1) * self.POP_SAMPLE_SIZE] for i in range(self.NUMBER_OF_SUBPOPS)]
        all_text = str(all_individuals)[1:-1] + '\n'
        for sub_pop in sub_pops:
            all_text += str(sub_pop)[1:-1] + '\n'
        leaves_text = ''
        for sub_pop in sub_pops:
            leaves_text += str(sub_pop)[1:-1] + '\n'
        common_text = f"----------- LEVEL 0 -----------\n Size_{self.OUTPUT_SIZE}_Level_0_Entry_0_Line_0_TH_0_Modularity" \
                      f"_       |A:{self.OUTPUT_SIZE}\n ----------- LEVEL 1 ----------- \n"
        for idx, sub_pop in enumerate(sub_pops):
            common_text += f"Size_{self.POP_SAMPLE_SIZE}_Level_1_Entry_0_Line_{idx}_ParentLevel_0_ParentEntry_0" \
                           f"_ParentLine_0_TH_0.2_Modularity_        |A:{self.POP_SAMPLE_SIZE}\n"
        vcf_dir = paths_helper.data_dir
        with open(vcf_dir + 'AllNodes.txt', 'w') as f:
            f.write(all_text)
        with open(vcf_dir + f'1_CommAnalysis_dynamic-false_modularity-true_minCommBrake-5_0.01.txt', 'w') as f:
            f.write(common_text)
        with open(vcf_dir + '2_Leafs_WithOverlap.txt', 'w') as f:
            f.write(leaves_text)

def simulation_runner(simulation_name):
    paths_helper = get_paths_helper(simulation_name)
    simulation = Simulation()
    simulation.add_simulation_to_data_config_file(paths_helper, simulation_name)
    simulation.run_simulation_and_save_vcf(paths_helper, simulation_name)
    simulation.copy_runner_to_vcf_dir(paths_helper)
    simulation.write_ind_list_for_ns(paths_helper)
    simulation.write_gt_ns_output(paths_helper)

    return True

if __name__ == '__main__':
    simulation = Simulation()
    print(f"simulation name is {sys.argv[1]}")
    paths_helper = get_paths_helper(sys.argv[1])
    simulation.write_gt_ns_output(paths_helper)
