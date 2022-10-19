import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

a = np.random.uniform(0, 80, 100).reshape(10,10)
b = np.concatenate([np.arange(10) for _ in range(10)]).reshape(10,10)
c = a + b
for i in range(10):
    plt.scatter([i] * 10, c[:, i])
d = np.concatenate([np.full(shape=10, fill_value=i) for i in range(10)])
polynomial = np.array([np.polyfit(d, c.T.flatten(), 1)])
plt.show()