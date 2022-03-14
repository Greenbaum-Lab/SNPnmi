import numpy as np

bin_to_int = {(False, False): -1,
              (False, True): 0,
              (True, False): 1,
              (True, True): 2}

int_to_bin = {-1: (False, False),
              0: (False, True),
              1: (True, False),
              2: (True, True)}


def numpy_compress(matrix):
    bin_mat = np.empty(shape=(matrix.shape[0], matrix.shape[1], 2), dtype=bool)
    bin_mat[:, :, 0] = matrix > 0
    bin_mat[:, :, 1] = matrix % 2 == 0
    return bin_mat


path = '/sci/labs/gilig/shahar.mazie/icore-data/vcf/sim_2_v0/classes/windows/maf_0.39/chr1/'

mat = np.load(path + 'window_93.012.npy')
# mat = np.array([[0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1],
#                 [2, 2, -1, 1, 2, 2, -1, 1, 2, 2, -1, 1, 2, 2, -1, 1],
#                 [0, -1, 0, -1, 0, -1, 2, 1, 1, -1, 0, 1, 0, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 1, 2, 1, -1, 0, 1, 0, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 0, -1, 0, -1, 0, 1, 1, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, 2, 0, 1]], dtype=np.int8)
bin_mat = numpy_compress(mat)
np.save(path + 'bin_exp.npy', bin_mat)
# np.save('/media/lab-heavy/Data/shahar/SNPnmi/tests/exp_bin.npy', bin_mat)
# np.save('/media/lab-heavy/Data/shahar/SNPnmi/tests/exp_int_np.npy', mat)

