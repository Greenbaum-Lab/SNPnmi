# python3 submit_merge_windows.py 10 2 49 49 1000 -1 False
import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, str2bool, get_paths_helper

JOB_TYPE = 'merge_windows'
#TODO add a get_cluster_path_to_wrapper()




python_to_run = 'python3 /cs/icore/amir.rubin2/code/snpnmi/utils/merge_windows_to_slices.py'
def submit_per_class(mac_maf, classes_names, paths_helper, num_windows_per_slice, num_slices, is_random):
    for class_name in classes_names:
        job_type=JOB_TYPE+('_random' if is_random else '_all')
        job_name=f'{mac_maf}_{class_name}'
        job_stderr_file=paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_name)
        job_stdout_file=paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_name)
        job_name=f'mrg_c{classes_names}'
        cluster_setting=f'sbatch --time=12:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
        cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} {python_to_run} {mac_maf} {class_name} {num_windows_per_slice} {num_slices} {is_random}'
        print(cmd_to_run)
        subprocess.run([paths_helper.submit_helper, cmd_to_run])

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    assert mac_min_range>=0
    mac_max_range = int(sys.argv[2])
    assert mac_max_range>=0

    # by maf
    maf_min_range = int(sys.argv[3])
    assert maf_min_range>=0
    maf_max_range = int(sys.argv[4])
    assert maf_max_range>=0

    # submission details
    num_windows_per_slice =  int(sys.argv[5])
    assert num_windows_per_slice>=0
    num_slices =  int(sys.argv[6])
    assert num_slices>=0
    is_random =  str2bool(sys.argv[7])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)

    print('num_windows_per_slice', num_windows_per_slice)
    print('num_slices', num_slices)
    print('is_random', is_random)

    paths_helper = get_paths_helper()

    submit_per_class('mac', range(mac_min_range, mac_max_range+1), paths_helper, num_windows_per_slice, num_slices, is_random)

    maf_classes_names = [str(maf/100) for maf in range(maf_min_range, maf_max_range+1)]

    submit_per_class('maf', maf_classes_names, paths_helper, num_windows_per_slice, num_slices, is_random)
