import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from sfs_analysis.sfs_utils import get_ticks_locations
a = np.random.normal(0, 1, 1000)
b = np.random.normal(0, 1, 150) * 2 + 1.5
plt.ylabel("Number of SNPs")
plt.xlabel("Minor Allele Count (MAC)")
plt.title("South Africa - Azerbaijan SFS")
colors = ['tab:blue', 'tab:orange']
print(a.shape)
print(b.shape)
sns.violinplot(data=[a, np.array(b)], palette=colors)
plt.xticks([0, 1], ['a', 'b'])
plt.show()
