#!/usr/bin/python3
import io
import os
import sys
from string import ascii_uppercase
from io import BytesIO
from os.path import dirname, abspath
import json
from PIL import Image
import cairosvg
import numpy as np
import subprocess
import matplotlib.pyplot as plt

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

import msprime
from utils.loader import Loader
from utils.common import get_paths_helper


class Simulation:
    def __init__(self, options=None):
        self.options = options
        self.output_size = 100
        self.population_size = 2000
        self.num_of_subpops = 2
        self.indv_per_pop = self.population_size // self.num_of_subpops
        self.pop_sample_size = self.output_size // self.num_of_subpops

    def run_simulation(self):
        demography = msprime.Demography()
        for i in range(self.num_of_subpops):
            demography.add_population(name=ascii_uppercase[i], initial_size=self.indv_per_pop)
        demography.add_population(name="AB", initial_size=self.population_size)
        demography.add_population_split(time=1000, derived=[e for e in ascii_uppercase[:self.num_of_subpops]], ancestral="AB")

        ts = msprime.sim_ancestry(samples={ascii_uppercase[i]: self.pop_sample_size for i in range(self.num_of_subpops)}, sequence_length=5e3, demography=demography,
                                  recombination_rate=1e-8, random_seed=1)
        mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=8e-7, random_seed=1)
        return mts

    @staticmethod
    def plot_tree(ts):
        _bytes = cairosvg.svg2png(bytestring=ts.draw_svg(),dpi=1280, write_to=open('demo.png', 'wb'))
        image = Image.open('demo.png')
        image.show()
        print()

    def run_simulation_and_save_vcf(self, paths_helper, simulation_name):
        with Loader("Running simulation"):
            mts = self.run_simulation()
        os.makedirs(paths_helper.data_dir, exist_ok=True)
        with Loader("Saving VCF"):
            with open(paths_helper.data_dir + simulation_name + '.vcf', 'w') as f:
                mts.write_vcf(f)

    def copy_runner_to_vcf_dir(self, paths_helper):
        SCRIPT_PATH = os.path.abspath(__file__)
        print(f"Copy {SCRIPT_PATH} to {paths_helper.data_dir}")
        subprocess.Popen(['cp', SCRIPT_PATH, paths_helper.data_dir])

    def write_ind_list_for_ns(self, paths_helper):
        with open(paths_helper.data_dir + 'sampleSite.txt', 'w+') as f:
            f.write('A')
        with open(paths_helper.data_dir + 'inlist.txt', 'w+') as f:
            f.write('A\n' * self.output_size)

    def add_simulation_to_data_config_file(self, paths_helper, simulation_name):
        data_json = paths_helper.repo + 'config/config.data.json'
        with open(data_json, "r") as f:
            js = json.load(f)
        assert simulation_name not in js, "Cannot overwrite simulations! First delete old simulation"
        js[simulation_name] = {'vcf_files_names': [f'{simulation_name}.vcf'],
                               'vcf_files_short_names': ['chr1'],
                               'num_chrs': 1,
                               'num_individuals': self.output_size,
                               'indlist_file_name': 'inlist.txt',
                               'sample_sites_file_name': 'sampleSite.txt'}
        with open(data_json, "w") as f:
            json.dump(js, f)

    def write_gt_ns_output(self, paths_helper):
        all_individuals = np.arange(self.output_size)
        sub_pops = [all_individuals[i * self.pop_sample_size: (i + 1) * self.pop_sample_size] for i in range(self.num_of_subpops)]
        all_text = str(all_individuals)[1:-1] + '\n'
        for sub_pop in sub_pops:
            all_text += str(sub_pop)[1:-1] + '\n'
        leaves_text = ''
        for sub_pop in sub_pops:
            leaves_text += str(sub_pop)[1:-1] + '\n'
        common_text = f"----------- LEVEL 0 -----------\n Size_{self.output_size}_Level_0_Entry_0_Line_0_TH_0_Modularity" \
                      f"_       |A:{self.output_size}\n ----------- LEVEL 1 ----------- \n"
        for idx, sub_pop in enumerate(sub_pops):
            common_text += f"Size_{self.pop_sample_size}_Level_1_Entry_0_Line_{idx}_ParentLevel_0_ParentEntry_0" \
                           f"_ParentLine_0_TH_0.2_Modularity_        |A:{self.pop_sample_size}\n"
        vcf_dir = paths_helper.data_dir
        with open(vcf_dir + 'AllNodes.txt', 'w') as f:
            f.write(all_text)
        with open(vcf_dir + f'1_CommAnalysis_dynamic-false_modularity-true_minCommBrake-{self.options.min_pop_size}_{self.options.ns_ss}.txt', 'w') as f:
            f.write(common_text)
        with open(vcf_dir + '2_Leafs_WithOverlap.txt', 'w') as f:
            f.write(leaves_text)


def simulation_runner(options):
    simulation_name = options.dataset_name
    paths_helper = get_paths_helper(simulation_name)
    simulation = Simulation(options)
    simulation.add_simulation_to_data_config_file(paths_helper, simulation_name)
    simulation.run_simulation_and_save_vcf(paths_helper, simulation_name)
    simulation.copy_runner_to_vcf_dir(paths_helper)
    simulation.write_ind_list_for_ns(paths_helper)
    simulation.write_gt_ns_output(paths_helper)

    return True

# if __name__ == '__main__':
#     simulation = Simulation()
#     print(f"simulation name is {sys.argv[1]}")
#     paths_helper = get_paths_helper(sys.argv[1])
#     simulation.write_gt_ns_output(paths_helper)
