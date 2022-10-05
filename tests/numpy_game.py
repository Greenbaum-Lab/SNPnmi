import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

M_RATES = (np.arange(100) + 1) / (10 ** 5)
GENERATIONS = np.arange(20) ** 2 + 1


a = np.arange(M_RATES.size * GENERATIONS.size).reshape(M_RATES.size, GENERATIONS.size)
fig, ax = plt.subplots(figsize=(12, 8))
title = "Heat Map of Peak scores"
plt.title(title, fontsize=18)
ttl = ax.title
ttl.set_position([0.5, 1.02])

plt.xlabel("Generations")
plt.ylabel("Migration rate")
ax.set_xticks(GENERATIONS)
s = sns.heatmap(a, fmt="", cmap='RdYlGn', linewidths=0.30, ax=ax, xticklabels=GENERATIONS,
            yticklabels=M_RATES)
s.set_xlabel('Generations', fontsize=16)
s.set_ylabel('Migration rate', fontsize=16)
ax.set_yticks(np.linspace(0, 99, 20, dtype=np.int))
plt.show()