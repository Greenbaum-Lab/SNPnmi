import numpy as np
import matplotlib.pyplot as plt
import sys

def get_similarity_distribution(similarity_matrix):
    a = 0.4
    bins = np.linspace(0.6, 0.95, 100)
    with open(similarity_matrix, 'rb') as f:
        matrix = np.load(f)
    max_e = np.max(matrix)
    matrix /= max_e
    vals = []
    for i in range(matrix.shape[0]):
        for j in range(i):
            vals.append(matrix[i, j])
    vals = np.array(vals)
    mean_vec = np.mean(matrix, axis=0)
    farthest_indv_idx = np.argmin(mean_vec)
    farthest_vals = matrix[farthest_indv_idx, np.arange(matrix.shape[0]) != farthest_indv_idx]
    closest_indv_idx = np.argmax(mean_vec)
    closest_vals = matrix[closest_indv_idx, np.arange(matrix.shape[0]) != closest_indv_idx]

    all_w = np.empty(vals.shape)
    all_w.fill(1/vals.shape[0])
    far_w = np.empty(farthest_vals.shape)
    far_w.fill(1/farthest_vals.shape[0])
    close_w = np.empty(closest_vals.shape)
    close_w.fill(1/closest_vals.shape[0])

    plt.hist([vals, closest_vals, farthest_vals], bins=bins, weights=[all_w, close_w, far_w], label=["All", "Most", "Least"], alpha=a, histtype='stepfilled')
    plt.legend(loc='upper left')
    plt.savefig("/home/lab2/shahar/cluster_dirs/tmp/similarity_distribution.png")
    plt.show()

if __name__ == '__main__':
    get_similarity_distribution("/home/lab2/shahar/cluster_dirs/vcf/hgdp_test/classes/similarity/all_mac_5-8_maf_46-49_similarity.npy")