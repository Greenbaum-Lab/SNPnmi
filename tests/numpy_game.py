from io import BytesIO
from PIL import Image
import numpy as np

import matplotlib.pyplot as plt

import cairosvg
import msprime


pop_configs = [msprime.PopulationConfiguration(sample_size=100),
               msprime.PopulationConfiguration(sample_size=100)]

ts = msprime.simulate(population_configurations=pop_configs, length=1e8, Ne=2000, mutation_rate=1e-6,
                      recombination_rate=1e-8,
                      demographic_events=[
                          msprime.MassMigration(10000, source=1, dest=0, proportion=1)
                      ])
with open("/home/lab2/shahar/sample_tmp.vcf", 'w+') as f:
    ts.write_vcf(f)
color_map = {0: 'red', 1: 'blue', 2: 'green'}
tree = ts.first()
node_colors = {u: color_map[tree.population(u)] for u in tree.nodes()}
img = cairosvg.svg2png(tree.draw(node_colours=node_colors))
img = Image.open(BytesIO(img))
plt.imshow(img)
plt.show()