#!/usr/bin/python3

import os
import sys
from string import ascii_uppercase, ascii_lowercase
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
            derived_pops = ['A', 'B'] if i == 0 else [ascii_lowercase[i - 1], ascii_uppercase[i + i]]
            demography.add_population(name=ascii_lowercase[i], initial_size=self.indv_per_pop * (i + 2))
            demography.add_population_split(time=self.generations_between_pops * (i + 1),
                                            derived=derived_pops, ancestral=ascii_lowercase[i])

        ts = msprime.sim_ancestry(
            samples={ascii_uppercase[i]: self.pop_sample_size for i in range(self.num_of_subpops)}, sequence_length=100,
            demography=demography,
            recombination_rate=.5, random_seed=1)
        mts = msprime.sim_mutations(ts, model=msprime.BinaryMutationModel(), rate=1, random_seed=1)
        return mts


if __name__ == '__main__':
    sim = SFSSimulation(ne=100, individuals_per_group=10,
                        num_of_groups=4,
                        generations_between_pops=100,
                        gene_flow_matrix=None)
    with Loader("Running simulation"):
        mts = sim.run_simulation()
    with Loader("saving VCF"):
        with open("/sci/labs/gilig/shahar.mazie/icore-data/sfs_proj/demo.vcf", 'w') as f:
            mts.write_vcf(f)
