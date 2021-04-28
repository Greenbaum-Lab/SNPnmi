# submit specific windows for mac 2
# python3 submit_calc_dist_windows.py 2 2 1 8 10 1 -1 -1 -1 True 1014 73511
# python3 submit_calc_dist_windows.py 2 18 1 1 49 1000 -1 0
# max done so far is: /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/count_dist_window_1013.tsv.gz
# max window is: /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/window_73511.012.tsv.gz
import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper


path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh'
calc_distances_in_window_cmd = 'python3 /cs/icore/amir.rubin2/code/snpnmi/utils/calc_distances_in_window.py'
job_type ='calc_dist_windows_mac_2'

# will submit calc_distances_in_window of given classes and windows
# python3 submit_calc_dist_windows.py 3 3 1 80 10 1 1 1 2148
def submit_calc_dist_windows(number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, use_specific_012_file ,min_input_012_file_index, max_input_012_file_index, max_windows_per_job=210):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)

    number_of_submitted_jobs = 0
    if use_specific_012_file:
        print('use_specific_012_file - currently only mac is supported!')
        if mac_min_range>0:
            print('go over mac values')
            for mac in range(mac_min_range, mac_max_range+1, mac_delta):
                # init
                job_max_input_012_file_index = min_input_012_file_index - 1
                while job_max_input_012_file_index < max_input_012_file_index:
                    # progress by max_windows_per_job
                    job_min_input_012_file_index = job_max_input_012_file_index + 1
                    # make sure we dont go over the max_input_012_file_index
                    job_max_input_012_file_index = min(job_min_input_012_file_index + max_windows_per_job, max_input_012_file_index)
                
                    if number_of_submitted_jobs == max_number_of_jobs:
                        break
                    job_long_name = f'mac{mac}_012_file{job_min_input_012_file_index}-{job_max_input_012_file_index}'
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                    job_name=f'c{mac}_{job_min_input_012_file_index}-{job_max_input_012_file_index}'
                    # it takes about 10 minutes to process each window. We have a max of 210 windows. This transalte to 35 hours. using 72 as a buffer.

                    cluster_setting=f'sbatch --time=72:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                    cmd_to_run=f'{cluster_setting} {path_to_wrapper} {calc_distances_in_window_cmd} mac {mac} -1 -1 -1 -1 {mac} {mac} True {job_min_input_012_file_index} {job_max_input_012_file_index}'
                    print(cmd_to_run)
                    subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                    number_of_submitted_jobs += 1
                    if number_of_submitted_jobs == max_number_of_jobs:
                        print(f'No more jobs will be submitted. Last submitted {job_min_input_012_file_index}-{job_max_input_012_file_index}')
                        break
        # if a specific input file is used, we wont go over macs and mafs
        return

    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)
    if mac_min_range>0:
        print('go over mac values')
        for mac in range(mac_min_range, mac_max_range+1, mac_delta):
            if number_of_submitted_jobs == max_number_of_jobs:
                break
            num_windows = class2num_windows[str(mac)]
            print(f'mac {mac}, num_windows {num_windows}')
            max_window_id = initial_window_index
            while max_window_id < num_windows:
                min_window_id = max_window_id
                max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows)
                # go over all windows
                job_long_name = f'fill_mac{mac}_windows{min_window_id}-{max_window_id}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                job_name=f'c{mac}_w{min_window_id}'
                cluster_setting=f'sbatch --time=48:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                cmd_to_run=f'{cluster_setting} {path_to_wrapper} {calc_distances_in_window_cmd} mac {mac} {min_window_id} {max_window_id} -1 -1 {mac} {mac}'
                print(cmd_to_run)
                subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted. Next window index to process is {max_window_id}')
                    break
    if maf_min_range>0:
        print('go over maf values')
        for maf_int in range(maf_min_range, maf_max_range+1, maf_delta):
            if number_of_submitted_jobs == max_number_of_jobs:
                break
            maf = f'{maf_int*1.0/100}'
            max_maf = f'{(maf_int + maf_delta)*1.0/100}' 
            num_windows = class2num_windows[maf]
            print(f'maf {maf}, num_windows {num_windows}')
            max_window_id = initial_window_index
            while max_window_id < num_windows:
                min_window_id = max_window_id
                max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows)
                # go over all windows
                job_long_name = f'fill_maf{maf}_windows{min_window_id}-{max_window_id}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                # to make the jobs name short we only take the last two digits of maf
                job_name=f'f{str(maf)[-2:]}_w{min_window_id}'
                cluster_setting=f'sbatch --time=12:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                #maf 0.49 0 0.49 0.5 -1 -1
                cmd_to_run=f'{cluster_setting} {path_to_wrapper} {calc_distances_in_window_cmd} maf {maf} {min_window_id} {max_window_id} {maf} {max_maf} -1 -1'
                print(cmd_to_run)
                subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted. Next window index to process is {max_window_id}')
                    break

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])
    mac_delta = int(sys.argv[3])

    # by maf
    maf_min_range = int(sys.argv[4])
    maf_max_range = int(sys.argv[5])
    maf_delta = int(sys.argv[6])

    # submission details
    number_of_windows_to_process_per_job =  int(sys.argv[7])
    max_number_of_jobs =  int(sys.argv[8])
    initial_window_index =  int(sys.argv[9])

    # use specific 012 input files
    # when true, for each mac/maf in range, we will process 012 files with index in range [min_input_012_file_index, max_input_012_file_index]
    use_specific_012_file = False
    min_input_012_file_index = -1
    max_input_012_file_index = -1

    if len(sys.argv) > 10:
        use_specific_012_file = bool(sys.argv[10])
    if use_specific_012_file:
        min_input_012_file_index = int(sys.argv[11])
        max_input_012_file_index = int(sys.argv[12])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('mac_delta', mac_delta)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('maf_delta', maf_delta)
    print('number_of_windows_to_process_per_job', number_of_windows_to_process_per_job)
    print('max_number_of_jobs', max_number_of_jobs)
    print('initial_window_index', initial_window_index)
    print('use_specific_012_file', use_specific_012_file)
    print('min_input_012_file_index', min_input_012_file_index)
    print('max_input_012_file_index', max_input_012_file_index)

# TODO support max_windows_per_job
    submit_calc_dist_windows(number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, use_specific_012_file ,min_input_012_file_index, max_input_012_file_index)