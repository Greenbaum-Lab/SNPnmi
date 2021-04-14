# python3 submit_fill_calc_dist_windows.py 2 18 1 49 1000
import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper


path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh'
fill_calc_distances_in_window_cmd = 'python3 /cs/icore/amir.rubin2/code/snpnmi/utils/fill_calc_distances_in_windows.py'
job_type ='fill_calc_distances_in_windows'

# will submit fill_calc_distances_in_windows of given classes - using window sized of 1000
def submit_fill_calc_distances_in_windows(number_of_windows_to_process_per_job, mac_min_range, mac_max_range, maf_min_range, maf_max_range):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)

    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>=0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                num_windows = class2num_windows[val]
                max_window_id = 0
                # if we have 10 windows, we can only process 0-9
                while max_window_id < num_windows-1:
                    min_window_id = max_window_id
                    max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows-1)
                    job_long_name = f'fill_calc_dist_{mac_maf}{val}_{min_window_id}-{max_window_id}'
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                    job_name=f'f{val}_{min_window_id}'
                    cluster_setting=f'sbatch --time=48:00:00 --mem=5G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                    cmd_to_run=f'{cluster_setting} {path_to_wrapper} {fill_calc_distances_in_window_cmd} {mac_maf} {val} {min_window_id} {max_window_id}'
                    print(cmd_to_run)
                    subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])


def main(args):
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    # by mac
    mac_min_range = int(args[0])
    mac_max_range = int(args[1])
    # by maf
    maf_min_range = int(args[2])
    maf_max_range = int(args[3])

    # submission details
    number_of_windows_to_process_per_job =  int(args[4])


    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('number_of_windows_to_process_per_job', number_of_windows_to_process_per_job)

    submit_fill_calc_distances_in_windows(number_of_windows_to_process_per_job, mac_min_range, mac_max_range, maf_min_range, maf_max_range)

#main(['-1', '1', '49', '49','1000'])

if __name__ == "__main__":
   main(sys.argv[1:])