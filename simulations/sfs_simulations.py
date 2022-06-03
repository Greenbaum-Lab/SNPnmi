#!/sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/python

import os
from os.path import dirname, abspath
import sys
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')

from string import ascii_uppercase, ascii_lowercase
import msprime
import matplotlib.pyplot as plt
from utils.scripts.freq_to_sfs import freq2sfs
from simulations.simulation_runner import Simulation
from utils.loader import Loader
import numpy as np
from utils.common import get_paths_helper


class SFSSimulation():
    def __init__(self, ne, pop_sizes, generations_between_pops, gene_flow_matrix):
        self.pop_sizes = pop_sizes
        self.output_size = np.sum(pop_sizes)
        self.population_size = ne
        self.num_of_subpops = pop_sizes.size
        self.generations_between_pops = generations_between_pops
        self.gene_flow_matrix = gene_flow_matrix

    def run_simulation(self):
        demography = msprime.Demography()
        for i in range(self.num_of_subpops):
            demography.add_population(name=ascii_uppercase[i], initial_size=self.population_size / self.num_of_subpops)
        for i in range(self.num_of_subpops - 1):
            derived_pops = ['A', 'B'] if i == 0 else [ascii_lowercase[i - 1], ascii_uppercase[i + 1]]
            demography.add_population(name=ascii_lowercase[i], initial_size=(self.population_size / self.num_of_subpops) * (i+2))
            demography.add_population_split(time=self.generations_between_pops * (i + 1),
                                            derived=derived_pops, ancestral=ascii_lowercase[i])

        ts_iterator = msprime.sim_ancestry(
            samples={ascii_uppercase[i]: self.pop_sizes[i] for i in range(self.num_of_subpops)}, num_replicates=1000,
            demography=demography, random_seed=1)
        mts = np.empty(0)
        for ts in ts_iterator:
            mt = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(),
                                             rate=1/(self.num_of_subpops * self.generations_between_pops), random_seed=1,
                                             discrete_genome=False)
            mt_matrix = np.array([e.genotypes for e in mt.variants()])
            if mt_matrix.size:
                mts = np.concatenate((mts, mt_matrix), axis=0) if mts.size else mt_matrix
        return mts

    def simulation_to_sfs(self):
        working_dir = '/sci/labs/gilig/shahar.mazie/icore-data/sfs_proj/demo/'
        os.system(f"vcftools --gzvcf {working_dir}demo.vcf --freq --out {working_dir}demo")
        macs_range = range(1, self.output_size * 2)
        mafs_range = []
        file_name = 'demo.frq'
        freq2sfs(macs_range=macs_range, mafs_range=mafs_range,
                 stats_dir=working_dir, file_name=file_name)

    def np_mutations_to_sfs(self, mts_numpy, pop_sizes):
        print(f"There are {mts_numpy.shape[0]} mutations")
        macs = mts_numpy.sum(axis=1)
        macs = np.minimum(macs, self.output_size * 2 - macs)
        min_bin = np.min(macs)
        max_bin = np.max(macs)
        hist = np.histogram(macs, bins=(max_bin - min_bin), density=False)
        assert np.all(hist[1] == hist[1].astype(int))
        plt.plot(np.arange(min_bin, max_bin), hist[0])
        plt.savefig(f"sfs_{str(pop_sizes).strip()}")
        plt.show()

if __name__ == '__main__':
    pop_sizes = np.array([6, 28, 14, 10])
    sim = SFSSimulation(ne=500, pop_sizes=pop_sizes,
                        generations_between_pops=400,
                        gene_flow_matrix=None)
    mts = sim.run_simulation()
    sim.np_mutations_to_sfs(mts, pop_sizes)

