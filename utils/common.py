import gzip

def get_number_of_windows_by_class(number_of_windows_per_class_path):
    class2num_windows = dict()
    with open(number_of_windows_per_class_path) as f:
        for l in f.readlines():
            classname, num_windows = l.split(' ',1)
            class2num_windows[classname] = int(num_windows)
    return class2num_windows


def build_empty_upper_left_matrix(n, default_value):
    return [[default_value]*(n-i) for i in range (1, n)]


def write_upper_left_matrix_to_file(output_file, values):
    with gzip.open(output_file,'wb') as f:
        for v in values:
            s = ' '.join([str(i) for i in v]) + '\n'
            f.write(s.encode())    