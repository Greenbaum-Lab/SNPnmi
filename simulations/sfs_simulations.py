#!/sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/python

import os
import sys
from string import ascii_uppercase, ascii_lowercase
from io import BytesIO
from os.path import dirname, abspath
import msprime


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.scripts.freq_to_sfs import freq2sfs
from simulations.simulation_runner import Simulation
from utils.loader import Loader
from utils.common import get_paths_helper


class SFSSimulation(Simulation):
    def __init__(self, ne, individuals_per_group, num_of_groups, generations_between_pops, gene_flow_matrix):
        Simulation.__init__(self)
        self.output_size = individuals_per_group * num_of_groups
        self.population_size = ne
        self.num_of_subpops = num_of_groups
        self.indv_per_pop = ne // num_of_groups
        self.pop_sample_size = self.output_size // num_of_groups
        self.generations_between_pops = generations_between_pops
        self.gene_flow_matrix = gene_flow_matrix

    def run_simulation(self):
        demography = msprime.Demography()
        for i in range(self.num_of_subpops):
            demography.add_population(name=ascii_uppercase[i], initial_size=self.indv_per_pop)
        for i in range(self.num_of_subpops - 1):
            derived_pops = ['A', 'B'] if i == 0 else [ascii_lowercase[i - 1], ascii_uppercase[i + 1]]
            demography.add_population(name=ascii_lowercase[i], initial_size=self.indv_per_pop * (i + 2))
            demography.add_population_split(time=self.generations_between_pops * (i + 1),
                                            derived=derived_pops, ancestral=ascii_lowercase[i])

        ts = msprime.sim_ancestry(
            samples={ascii_uppercase[i]: self.pop_sample_size for i in range(self.num_of_subpops)}, sequence_length=1300,
            demography=demography,
            recombination_rate=.5, random_seed=1)
        mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=1, random_seed=1)
        return mts

    def simulation_to_sfs(self):
        working_dir = '/sci/labs/gilig/shahar.mazie/icore-data/sfs_proj/demo/'
        # subprocess.Popen(f"vcftools --gzvcf {working_dir}demo.vcf --freq --out {working_dir}demo")
        macs_range = range(2, 71)
        mafs_range = range(1, 51)
        file_name = 'demo.frq'
        freq2sfs(macs_range=macs_range, mafs_range=mafs_range,
                 stats_dir=working_dir, file_name=file_name)


if __name__ == '__main__':
    sim = SFSSimulation(ne=100, individuals_per_group=10,
                        num_of_groups=4,
                        generations_between_pops=100,
                        gene_flow_matrix=None)
    with Loader("Running simulation"):
        mts = sim.run_simulation()
    with Loader("saving VCF"):
        with open("/sci/labs/gilig/shahar.mazie/icore-data/sfs_proj/demo/demo.vcf", 'w') as f:
            mts.write_vcf(f)
    sim.simulation_to_sfs()

