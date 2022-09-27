
import os
import time
from os.path import dirname, abspath
import sys

from tqdm import tqdm


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')

from utils.cluster.cluster_helper import submit_to_cluster
from utils.loader import Loader
from utils.common import args_parser, get_paths_helper, warp_how_many_jobs, validate_stderr_empty
from string import ascii_uppercase, ascii_lowercase
import msprime
import matplotlib.pyplot as plt
from utils.scripts.freq_to_sfs import freq2sfs
import numpy as np
import json


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
            if self.time_to_mass_migration > 0:
                demography.add_mass_migration(time=self.time_to_mass_migration, source='A', dest='B', proportion=0.2)
                demography.add_mass_migration(time=self.time_to_mass_migration, source='B', dest='A', proportion=0.2)
            demography.add_population_split(time=self.generations_between_pops * (i + 1),
                                            derived=derived_pops, ancestral=ascii_lowercase[i])
            # demography.set_symmetric_migration_rate(['A', 'B'], self.migration_rate)

        mts = np.empty(0)
        while mts.shape[0] < self.num_of_snps:
            ts = msprime.sim_ancestry(
            samples={ascii_uppercase[i]: self.pop_sizes[i] for i in range(self.num_of_subpops)},
            demography=demography)
            mt = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(),
                                             rate=1/(self.population_size * 2),
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

    def np_mutations_to_sfs(self, mts_numpy):
        macs = mts_numpy.sum(axis=1)
        macs = np.minimum(macs, self.output_size * 2 - macs)
        hist = np.histogram(macs, bins=np.arange(self.output_size + 2), density=False)
        assert np.all(hist[1] == hist[1].astype(int))
        return hist[0]


def sfs2R(sfs, hot_spot):
    return sfs[hot_spot] / np.max(1, np.sqrt(sfs[hot_spot - 1] * sfs[hot_spot + 1]))


def plot_by_generations(options, plots_base_dir, migration_rate, single_plot=False):
    pop_sizes = np.array([8, 12])
    iterations = 5
    gens = np.arange(20) ** 2 + 1
    hot_spot = int(np.min(pop_sizes) * 2)
    gens2R_mean = np.empty(shape=gens.size)
    gens2R_var = np.empty(shape=gens.size)

    for idx, generations_between_pops in enumerate(gens):
        hot_spots_per_gen = np.empty(shape=iterations)
        for iter in range(iterations):
            sim = SFSSimulation(options=options, ne=200, pop_sizes=pop_sizes,
                                generations_between_pops=generations_between_pops,
                                migration_rate=migration_rate,
                                num_of_snps=200,
                                time_to_mass_migration=0)
            mts = sim.run_simulation()
            sfs = sim.np_mutations_to_sfs(mts)

            hot_spots_per_gen[iter] = sfs2R(sfs, hot_spot)
        gens2R_mean[idx] = np.mean(hot_spots_per_gen)
        gens2R_var[idx] = np.var(hot_spots_per_gen)
    if single_plot:
        plt.plot(gens, gens2R_mean)
        plt.xlabel(f"Generations since split", fontsize=16)
        plt.ylabel("Relatives Peak", fontsize=16)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.title("Relatives Peak increase with generations since split", fontsize=16)
        plt.fill_between(gens, y1=gens2R_mean - gens2R_var, y2=gens2R_mean + gens2R_var,
                         alpha=0.3)
        plt.savefig(plots_base_dir + 'generations.svg')
    with open(plots_base_dir + f'm_{migration_rate}.json', "w") as f:
        json.dump([float(e) for e in gens2R_mean], f)


def submit_all_migration_rates(options, paths_helper):
    m_rates = (np.arange(10) + 1) / (10 ** 4)
    job_type = 'simulations_job'
    script_path = os.path.abspath(__file__)
    errs = []
    for m in m_rates:
        job_name = f'm_{m}'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_name)
        errs.append(job_stderr_file)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_name)
        submit_to_cluster(options, job_type, job_name, script_path, f"--args {m}", job_stdout_file, job_stderr_file,
                          num_hours_to_run=24, memory=16, use_checkpoint=True)

    jobs_func = warp_how_many_jobs('m_')
    with Loader("Simulationg coalecent simulations", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(errs)
    print("Done!")

if __name__ == '__main__':
    options = args_parser()
    options.dataset_name = 'simulations'
    plots_base_dir = '/sci/labs/gilig/shahar.mazie/icore-data/sfs_proj/sfs_plots/'
    os.makedirs(plots_base_dir, exist_ok=True)
    paths_helper = get_paths_helper(options.dataset_name)
    if not options.args:
        submit_all_migration_rates(options, paths_helper)
    else:
        m = float(options.args[0])
        plot_by_generations(options, plots_base_dir, migration_rate=m)
