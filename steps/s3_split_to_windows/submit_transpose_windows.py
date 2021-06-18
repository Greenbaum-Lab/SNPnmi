import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper

# will submit transpose_windows of given classes and windows

# python3 submit_transpose_windows.py 2 -1 2 4 144 72
transpose_windows_python_call = 'python3 /cs/icore/amir.rubin2/code/snpnmi/utils/transpose_windows.py'


def submit_transpose_windows(mac, maf, window_id, first_index_to_use, expected_number_of_files):
    paths_helper = get_paths_helper()
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type='transpose_windows', job_name=str(window_id))
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type='transpose_windows', job_name=str(window_id))
    os.makedirs(dirname(os.path.abspath(job_stderr_file)), exist_ok=True)
    os.makedirs(dirname(os.path.abspath(job_stdout_file)), exist_ok=True)
    # go over all windows
    class_name = max(int(mac), float(maf))
    job_name=f't{class_name}w{window_id}'
    cluster_setting=f'sbatch --time=48:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} {transpose_windows_python_call} {mac} {maf} {window_id} {first_index_to_use} {expected_number_of_files}'
    print(cmd_to_run)
    subprocess.run([paths_helper.submit_helper, cmd_to_run])

def main(args):
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac = args[0]
    maf = args[1]
    min_input_window_id = int(args[2])
    max_input_window_id = int(args[3])
    first_index_to_use = int(args[4])
    expected_number_of_files_per_input_window = int(args[5])
    print('mac',mac)
    print('maf',maf)
    print('min_input_window_id',min_input_window_id)
    print('max_input_window_id',max_input_window_id)
    print('first_index_to_use',first_index_to_use)
    print('expected_number_of_files_per_input_window',expected_number_of_files_per_input_window)


    for window_id in range(min_input_window_id,max_input_window_id+1):
        submit_transpose_windows(mac, maf, window_id, first_index_to_use, expected_number_of_files_per_input_window)
        # each time we increase the first index to use as output by the number of files in each input window
        first_index_to_use += expected_number_of_files_per_input_window

# #params
# mac = -1
# maf = 0.49
# min_input_window_id = 1
# max_input_window_id = 2
# first_index_to_use = 72
# expected_number_of_files_per_input_window = 72
# main([mac, maf, window_id, first_index_to_use, expected_number_of_files])

if __name__ == "__main__":
    main(sys.argv[1:])