import numpy as np
import matplotlib.pyplot as plt
import sys

def get_similarity_distribution(similarity_matrix):
    with open(similarity_matrix, 'rb') as f:
        matrix = np.load(f)
    max_e = np.max(matrix)
    vals = []
    for i in range(matrix.shape[0]):
        for j in range(i):
            vals.append(matrix[i, j] / max_e)
    plt.hist(vals, bins=100)
    plt.savefig("/vol/sci/bio/data/gil.greenbaum/shahar.mazie/tmp/similarity_distribution.png")
    plt.show()
