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

def numpy_decompress(bin_mat):
    decomp_mat = np.empty(shape=(bin_mat.shape[0], bin_mat.shape[1]), dtype=np.int8)
    decomp_mat[:, :] = bin_mat[:, :, 0] & ~ bin_mat[:, :, 1]
    decomp_mat[:, :] -= ~bin_mat[:, :, 0] & ~ bin_mat[:, :, 1]
    decomp_mat[:, :] += 2 * (bin_mat[:, :, 0] & bin_mat[:, :, 1])
    return decomp_mat


path = '/sci/labs/gilig/shahar.mazie/icore-data/vcf/sim_2_v0/classes/windows/maf_0.39/chr1/'

mat = np.load(path + 'window_93.012.npy')
# mat = np.array([[0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1],
#                 [2, 2, -1, 1, 2, 2, -1, 1, 2, 2, -1, 1, 2, 2, -1, 1],
#                 [0, -1, 0, -1, 0, -1, 2, 1, 1, -1, 0, 1, 0, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 1, 2, 1, -1, 0, 1, 0, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 0, -1, 0, -1, 0, 1, 1, -1, 0, 1],
#                 [0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, 2, 0, 1]], dtype=np.int8)
compressed = numpy_compress(mat)
bin_mat = np.packbits(compressed)
np.save(path + 'bin_exp.npy', bin_mat)
# np.save('/media/lab-heavy/Data/shahar/SNPnmi/tests/exp_bin.npy', bin_mat)
# np.save('/media/lab-heavy/Data/shahar/SNPnmi/tests/exp_int_np.npy', mat)

# bin_mat_load = np.load('/media/lab-heavy/Data/shahar/SNPnmi/tests/exp_bin.npy')
bin_mat_load = np.load(path + 'bin_exp.npy')
unpacked = np.unpackbits(bin_mat).astype(np.bool).reshape((mat.shape[0], mat.shape[1], 2))
renew = numpy_decompress(unpacked)
print(np.all(renew == mat))