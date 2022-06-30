
import os
from os.path import dirname, abspath
import sys

import gif
from tqdm import tqdm

from utils.common import args_parser, get_paths_helper

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')

from string import ascii_uppercase, ascii_lowercase
import msprime
import matplotlib.pyplot as plt
from utils.scripts.freq_to_sfs import freq2sfs
import numpy as np


class SFSSimulation():
    def __init__(self,options, ne, pop_sizes, generations_between_pops, migration_rate, num_of_snps, time_to_mass_migration):
        self.options = options
        self.pop_sizes = pop_sizes
        self.output_size = np.sum(pop_sizes)
        self.population_size = ne
        self.num_of_subpops = pop_sizes.size
        self.generations_between_pops = generations_between_pops
        self.migration_rate = migration_rate
        self.num_of_snps = num_of_snps
        self.time_to_mass_migration = time_to_mass_migration

    def run_simulation(self):
        demography = msprime.Demography()
        for i in range(self.num_of_subpops):
            demography.add_population(name=ascii_uppercase[i], initial_size=self.population_size / self.num_of_subpops)
        for i in range(self.num_of_subpops - 1):
            derived_pops = ['A', 'B'] if i == 0 else [ascii_lowercase[i - 1], ascii_uppercase[i + 1]]
            demography.add_population(name=ascii_lowercase[i], initial_size=(self.population_size / self.num_of_subpops) * (i+2))
            demography.add_mass_migration(time=self.time_to_mass_migration, source='A', dest='B', proportion=0.2)
            demography.add_mass_migration(time=self.time_to_mass_migration, source='B', dest='A', proportion=0.2)
            demography.add_population_split(time=self.generations_between_pops * (i + 1),
                                            derived=derived_pops, ancestral=ascii_lowercase[i])
            demography.set_symmetric_migration_rate(['A', 'B'], self.migration_rate)

        mts = np.empty(0)
        while mts.shape[0] < self.num_of_snps:
            ts = msprime.sim_ancestry(
            samples={ascii_uppercase[i]: self.pop_sizes[i] for i in range(self.num_of_subpops)},
            demography=demography)
            mt = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(),
                                             rate=1/(self.num_of_subpops * self.generations_between_pops * 2), random_seed=1,
                                             discrete_genome=False)
            mt_matrix = np.array([e.genotypes for e in mt.variants()])
            if mt_matrix.size:
                snp_rand_idx = np.random.randint(mt_matrix.shape[0])
                single_snp_matrix = mt_matrix[snp_rand_idx].reshape(1, -1)
                mts = np.concatenate((mts, single_snp_matrix), axis=0) if mts.size else mt_matrix
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
        macs = mts_numpy.sum(axis=1)
        macs = np.minimum(macs, self.output_size * 2 - macs)
        min_bin = np.min(macs)
        max_bin = np.max(macs)
        hist = np.histogram(macs, bins=(max_bin - min_bin), density=False)
        assert np.all(hist[1] == hist[1].astype(int))
        return min_bin, max_bin, hist[0]

@gif.frame
def plot_sfs_with_std(sfs, min_bin, max_bin, paths_helper, time_to_mass_migration):
    average = sfs.mean(axis=0)
    std = np.std(sfs, axis=0)
    plt.plot(np.arange(min_bin, max_bin), average)
    plt.fill_between(np.arange(min_bin, max_bin), y1=average - std, y2=average + std, alpha=0.3)
    plt.title(f"Time to pulse: {time_to_mass_migration} ")
    plt.xlabel("Minor allele count")
    plt.ylabel("Number of SNPs")
    # plt.savefig(f"{paths_helper.sfs_proj}shifting_migration_window_plots/sfs_{time_to_mass_migration}.svg")
    # plt.clf()


if __name__ == '__main__':
    options = args_parser()
    paths_helper = get_paths_helper(options.dataset_name)
    pop_sizes = np.array([10, 20])
    frames_for_gif = []
    for time_to_mass_migration in tqdm([0, 1, 2,3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 50, 100, 150, 200, 300]):
        # if os.path.exists(f"{paths_helper.sfs_proj}shifting_migration_window_plots/sfs_{time_to_mass_migration}.svg"):
        #     continue
        sfs_over_iterations = []
        prev_min_bin = None
        prev_max_bin = None
        for iter in tqdm(range(10), leave=False):
            sim = SFSSimulation(options=options, ne=250, pop_sizes=pop_sizes,
                                generations_between_pops=400,
                                migration_rate=0,
                                num_of_snps=2000,
                                time_to_mass_migration=time_to_mass_migration)
            mts = sim.run_simulation()
            min_bin, max_bin, sfs = sim.np_mutations_to_sfs(mts, pop_sizes)
            if prev_min_bin is not None:
                assert min_bin == prev_min_bin
                assert max_bin == prev_max_bin
            prev_max_bin = max_bin
            prev_min_bin = min_bin
            sfs_over_iterations.append(sfs)
        sfs_over_iterations = np.array(sfs_over_iterations)
        frames_for_gif.append(plot_sfs_with_std(sfs_over_iterations, prev_min_bin, prev_max_bin, paths_helper, time_to_mass_migration))
    gif.save(frames_for_gif, f"{paths_helper.sfs_proj}shifting_migration_window_plots/all.gif", duration=30, unit='s')
