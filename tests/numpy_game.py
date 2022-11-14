import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from sfs_analysis.sfs_utils import get_ticks_locations

plt.scatter([0,1,2], [3,2,1])
plt.legend(handles=[Line2D([0], [0], color='w', markerfacecolor='tab:blue', marker='o', label='General'),
                    Line2D([0], [0], color='w', markerfacecolor='tab:orange', marker='o', label='Africa')])
plt.show()