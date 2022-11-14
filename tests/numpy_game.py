import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from sfs_analysis.sfs_utils import get_ticks_locations

a = (np.random.random(30) * 2 + np.arange(30) ** 0.5) / 10
plt.plot(a)
plt.fill_between(x=np.arange(30),y1=a - .1, y2=a+.1, alpha=0.3)
plt.xlabel("MAC")
plt.ylabel("NMI score")
plt.show()
enumerate()