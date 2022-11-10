
import os
from os.path import dirname, abspath
import sys

from tqdm import tqdm


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')

from utils.s7_join_to_summary.plots_helper import r2score, heatmap_plot
from utils.cluster.cluster_helper import submit_to_cluster
from utils.loader import wait_and_validate_jobs
from utils.common import args_parser, get_paths_helper, repr_num
from string import ascii_uppercase, ascii_lowercase
import msprime
import matplotlib.pyplot as plt
from utils.scripts.freq_to_sfs import freq2sfs
import numpy as np
import json

DEBUG = False
M_RATES = (np.arange(100) + 1) / (10 ** 6)
# M_RATES = np. array([0, 10 ** -5, 10 ** -4, 10 ** -3, 10 ** -2])
# GENERATIONS = np.arange(20) ** 2 + 1
GENERATIONS = np.linspace(1, 401, 41).astype(int)
BOUND_SAMPLE_SIZE = [1, 6] if DEBUG else [1, 30]
pop_sizes_range = np.arange(BOUND_SAMPLE_SIZE[0], BOUND_SAMPLE_SIZE[1] + 1)
ITERATIONS = 8 if DEBUG else 100
CONST_GENERATIONS = 300
CONST_MIGRATION = 10 ** -5
CONST_NUM_OF_SNPS = 200 if DEBUG else 2000

job_type = 'simulations_job'
script_path = os.path.abspath(__file__)

class SFSSimulation():
    def __init__(self,options, ne, pop_sizes, generations_between_pops, migration_rate, num_of_snps, time_to_mass_migration=0):
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
            demography.set_symmetric_migration_rate(['A', 'B'], self.migration_rate)

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
    sfs[-1] *= 2
    return sfs[hot_spot] / np.max([1, np.sqrt(sfs[hot_spot - 1] * sfs[hot_spot + 1])])


def simulate_different_pop_sizes(options, plots_base_dir, pop1_size, single_plot=False):
    output_path = plots_base_dir + f'p_{pop1_size}.json'
    if os.path.exists(output_path):
        with open(output_path, "rb") as f:
            pop2res = json.load(f)
    else:
        pop2res = {}
    for idx, pop2_size in enumerate(pop_sizes_range):
        if str(pop2_size) in pop2res:
            continue
        if pop2_size == pop1_size:
            pop2res[int(pop2_size)] = [np.nan, np.nan]
            continue
        pop_sizes = np.array([pop1_size, pop2_size])
        hot_spot = np.min(pop_sizes) * 2
        hot_spots_per_pop = np.empty(shape=ITERATIONS)
        for iter in range(ITERATIONS):
            sim = SFSSimulation(options=options, ne=200, pop_sizes=pop_sizes,
                                generations_between_pops=CONST_GENERATIONS,
                                migration_rate=CONST_MIGRATION,
                                num_of_snps=CONST_NUM_OF_SNPS)
            mts = sim.run_simulation()
            sfs = sim.np_mutations_to_sfs(mts)

            hot_spots_per_pop[iter] = sfs2R(sfs, hot_spot)
        pop2res[int(pop2_size)] = [np.mean(hot_spots_per_pop), np.var(hot_spots_per_pop)]
    if single_plot:
        mean = np.array([e[0] for e in pop2res.values()])
        var = np.array([e[1] for e in pop2res.values()])
        plt.plot(pop2res.keys(), [e[0] for e in pop2res.values()])
        plt.xlabel(f"pop2 size", fontsize=16)
        plt.ylabel("Relatives Peak", fontsize=16)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.title(f"Relatives Peak. Population 1 size fixed to {pop1_size}", fontsize=16)
        plt.fill_between(pop2res.keys(), y1=mean - var, y2=mean + var,
                         alpha=0.3)
        plt.savefig(plots_base_dir + f'pop1_{pop1_size}.svg')
    with open(output_path, "w") as f:
        json.dump(pop2res, f)


def plot_by_generations(options, plots_base_dir, migration_rate, single_plot=False):
    pop_sizes = np.array([8, 12])

    hot_spot = np.min(pop_sizes) * 2
    gens2R_mean = np.empty(shape=GENERATIONS.size)
    gens2R_var = np.empty(shape=GENERATIONS.size)

    for idx, generations_between_pops in enumerate(GENERATIONS):
        print(f"Done with {idx} out of 20")
        hot_spots_per_gen = np.empty(shape=ITERATIONS)
        for iter in range(ITERATIONS):
            sim = SFSSimulation(options=options, ne=200, pop_sizes=pop_sizes,
                                generations_between_pops=generations_between_pops,
                                migration_rate=migration_rate,
                                num_of_snps=CONST_NUM_OF_SNPS,
                                time_to_mass_migration=0)
            mts = sim.run_simulation()
            sfs = sim.np_mutations_to_sfs(mts)

            hot_spots_per_gen[iter] = sfs2R(sfs, hot_spot)
        gens2R_mean[idx] = np.mean(hot_spots_per_gen)
        gens2R_var[idx] = np.var(hot_spots_per_gen)
    if single_plot:
        plt.plot(GENERATIONS, gens2R_mean)
        plt.xlabel(f"Generations since split", fontsize=16)
        plt.ylabel("Relatives Peak", fontsize=16)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.title("Relatives Peak increase with generations since split", fontsize=16)
        plt.fill_between(GENERATIONS, y1=gens2R_mean - gens2R_var, y2=gens2R_mean + gens2R_var,
                         alpha=0.3)
        plt.savefig(plots_base_dir + 'generations.svg')
    with open(plots_base_dir + f'm_{migration_rate}.json', "w") as f:
        json.dump([float(e) for e in gens2R_mean], f)
    with open(plots_base_dir + f'm_{migration_rate}_var.json', "w") as f:
        json.dump([float(e) for e in gens2R_var], f)


def submit_all_migration_rates(options, paths_helper, plots_base_dir):
    errs = []
    for m in M_RATES:
        job_name = f'm_{m}'
        if os.path.exists(f"{plots_base_dir}{job_name}.json"):
            continue
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_name)
        errs.append(job_stderr_file)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_name)
        submit_to_cluster(options, job_type, job_name, script_path, f"--args {m} -s {options.step}",
                          job_stdout_file, job_stderr_file, num_hours_to_run=24, memory=16, use_checkpoint=True)
    if len(errs) == 0:
        return

    wait_and_validate_jobs('m_', "Simulating coalescent simulations", errs)


def submit_all_sample_sizes(options, paths_helper, plots_base_dir):
    errs = []
    for pop1_size in pop_sizes_range:
        job_name = f'p_{pop1_size}'
        output_name = f"{plots_base_dir}{job_name}.json"
        if os.path.exists(output_name):
            with open(output_name, "rb") as f:
                dict = json.load(f)
            if all([str(e) in dict for e in pop_sizes_range]):
                print(f"file exists for populatoin size {pop1_size}")
                continue

        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_name)
        errs.append(job_stderr_file)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_name)
        submit_to_cluster(options, job_type, job_name, script_path, f"--args {pop1_size} -s {options.step}",
                          job_stdout_file, job_stderr_file, num_hours_to_run=24, memory=16, use_checkpoint=True)

    wait_and_validate_jobs('p_', "Simulating coalescent simulations", errs)


def combine_migration_json2heatmap(plots_base_dir):

    all_peak_scores = []
    for m in tqdm(M_RATES):
        path = f"{plots_base_dir}m_{m}.json"
        with open(path, "rb") as f:
            all_peak_scores.append(json.load(f))
    peak_scores = np.array(all_peak_scores)
    np.save(f"{plots_base_dir}migration_heatmap.npy", peak_scores)
    heatmap_plot(output=f"{plots_base_dir}migration_heatmap.svg",
                 data_matrix=peak_scores,
                 x_label='Generations',
                 y_label='Migration rate',
                 x_ticks=GENERATIONS,
                 y_ticks=M_RATES,
                 c_bar_label='Peak score',
                 title="Heat Map of Peak scores",
                 y_bins=10,
                 x_bins=5)


def combine_sample_size2heatmap(plots_dir):
    all_peak_scores = []
    for p1 in tqdm(pop_sizes_range):
        path = f"{plots_dir}p_{p1}.json"
        with open(path, "rb") as f:
            current_p1_scores = json.load(f)
            all_peak_scores.append([current_p1_scores[str(p2)][0] for p2 in pop_sizes_range])
    peak_scores = np.array(all_peak_scores)
    np.save(f"{plots_dir}ss_heatmap.npy", peak_scores)
    heatmap_plot(output=f"{plots_dir}ss_heatmap_fig.svg",
                 data_matrix=peak_scores,
                 x_label='Sample size of population 1',
                 y_label='Sample size of population 1',
                 x_ticks=pop_sizes_range,
                 y_ticks=pop_sizes_range,
                 c_bar_label='Peak score',
                 title="Heat Map of Peak scores",
                 y_bins=20,
                 x_bins=20)


def combine_json2_migrations_plot(plots_base_dir):
    colors = ['tab:blue', 'tab:red', 'tab:orange', 'tab:green', 'tab:cyan', 'tab:yellow']
    for i, m in enumerate(tqdm(M_RATES)):
        mean_path = f"{plots_base_dir}m_{m}.json"
        with open(mean_path, "rb") as f:
            mean_vals = np.array(json.load(f))
        var_path = f"{plots_base_dir}m_{m}_var.json"
        with open(var_path, "rb") as f:
            var_vals = np.array(json.load(f))
        plt.plot(GENERATIONS, mean_vals, color=colors[i], label=m)
        plt.fill_between(GENERATIONS, y1=mean_vals - var_vals, y2=mean_vals + var_vals,
                         alpha=0.3, color=colors[i])
    plt.title("Peak score with different migration rates")
    plt.xlabel("Generations")
    plt.ylabel("Peak score")
    plt.legend(title="Migration rates")
    plt.savefig(f"{plots_base_dir}plot.svg")


def combine_json2sample_size_plot(output_dir):
    heatmap = np.load(f"{output_dir}ss_heatmap.npy")
    res = [[] for _ in range(heatmap.shape[0])]
    for i, row in enumerate(heatmap):
        for j, val in enumerate(row):
            if not np.isnan(val):
                res[np.min([i, j])].append(val)
    x_vals = np.concatenate([np.full_like(e, fill_value=(idx + 1) * 2, dtype=int) for idx, e in enumerate(res)])
    y_vals = np.concatenate(res)
    plt.scatter(x=x_vals, y=y_vals)
    poly = np.array(np.polyfit(x_vals, y_vals, 1))
    p = np.poly1d(poly)
    y_hat = p(x_vals)
    plt.plot(x_vals, y_hat, linestyle='--')
    text = f"$y={repr_num(poly[0])}x + {repr_num(poly[1])}$\n$R^2 = {repr_num(r2score(y_vals, y_hat))}$"
    plt.gca().text(.01, .99, text, transform=plt.gca().transAxes,
                   fontsize=10, verticalalignment='top')
    plt.title("Peak score correlation to hot spot value", fontsize=18)
    plt.xlabel("Hot-spot", fontsize=16)
    plt.ylabel("Peak score", fontsize=16)
    plt.savefig(f"{output_dir}correlation.svg")

def manage_migration_runs(options, paths_helper, base_dir):
    output_dir = base_dir + 'migrations_new_gens/'
    os.makedirs(output_dir, exist_ok=True)
    if not options.args:
        os.makedirs(output_dir, exist_ok=True)
        submit_all_migration_rates(options, paths_helper, output_dir)
        if len(M_RATES) > 6:
            combine_migration_json2heatmap(output_dir)
        else:
            combine_json2_migrations_plot(output_dir)
    else:
        m = float(options.args[0])
        plot_by_generations(options, output_dir, migration_rate=m)


def manage_sample_size_runs(options, paths_helper, base_dir):
    output_dir = base_dir + 'sample_size_grid/'
    os.makedirs(output_dir, exist_ok=True)
    if not options.args:
        os.makedirs(output_dir, exist_ok=True)
        submit_all_sample_sizes(options, paths_helper, output_dir)
        combine_sample_size2heatmap(output_dir)
        combine_json2sample_size_plot(output_dir)
    else:
        p1 = int(options.args[0])
        simulate_different_pop_sizes(options, output_dir, pop1_size=p1)


if __name__ == '__main__':
    options = args_parser()
    options.dataset_name = 'simulations'
    paths_helper = get_paths_helper(options.dataset_name)
    base_dir = paths_helper.sfs_proj
    if options.step == '1':
        manage_migration_runs(options, paths_helper, base_dir)
    elif options.step == '2':
        manage_sample_size_runs(options, paths_helper, base_dir)
    else:
        raise KeyError("No step was provided")
