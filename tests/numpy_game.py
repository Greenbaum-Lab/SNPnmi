import numpy as np
import matplotlib.pyplot as plt
import json

# path = '/home/data1/shahar/sim_dip_v0/mafs_macs_dicts.json'
# with open(path, 'r') as f:
#     dicts = json.load(f)
# mafs = dicts[0]
# macs = dicts[1]
# plt.plot(np.arange(2, 71), list(macs.values())[1:])
# plt.title('mac')
# plt.yscale('log')
# plt.savefig('/home/data1/shahar/sim_dip_v0/mac_plot.svg')
# plt.clf()
#
# maf = list(mafs)
# maf[-2] += maf[-1]
# plt.plot(np.arange(1, 50)/100, list(mafs.values())[:-1])
# plt.title('maf')
# plt.yscale('log')
# plt.savefig('/home/data1/shahar/sim_dip_v0/maf_plot.svg')

N = 200
mu = 10e-6
theta = 4 * N * mu
dist = []
for i in range(1,101):
    dist.append(theta/i + theta/(N - i))
plt.plot((np.arange(100) + 1)/N, dist)
plt.title("Expected SFS")
plt.yticks([])
plt.xlabel("Site frequency class")
plt.show()
