# python3 fill_calc_distances_in_windows.py maf 0.49 0 10

import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from datetime import datetime
import gzip
from utils.common import get_paths_helper
from utils.common import get_number_of_windows_by_class
from utils.config import get_num_individuals
import subprocess
from utils.validate import _validate_count_dist_file

def _submit_calc_dist_job(mac_maf, class_name, i):
    job_type ='fill_calc_dist_window'
    
    calc_distances_in_window_cmd = 'python3 /cs/icore/amir.rubin2/code/snpnmi/utils/calc_distances_in_window.py'
    paths_helper = get_paths_helper()
    job_long_name = f'fill_{mac_maf}_{class_name}_window_{i}'
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
    os.makedirs(dirname(job_stderr_file), exist_ok=True)

    job_name=f'{class_name}_w{i}'
    cluster_setting=f'sbatch --time=48:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    if mac_maf == 'mac':
        # in mac 2 we use a specific input file
        if class_name == '2':
            cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} {calc_distances_in_window_cmd} mac {class_name} {i} {int(i)+1} -1 -1 -1 -1 True {i} {i}'
        else:
            cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} {calc_distances_in_window_cmd} mac {class_name} {i} {int(i)+1} -1 -1 {class_name} {class_name}'
    else:
        max_maf = f'{float(class_name) + 0.01}'
        cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} {calc_distances_in_window_cmd} maf {class_name} {i} {int(i)+1} {class_name} {max_maf} -1 -1'

    print(cmd_to_run)
    subprocess.run([paths_helper.submit_helper, cmd_to_run])

def _log_job_submitted(fill_log_file, mac_maf, class_name, i):
    with open(fill_log_file, 'a') as logf:
        logf.write(f'{mac_maf},{class_name},{i}\n')


# We will go over all count_dist files in [min_index_to_fill, max_index_to_fill].
# First we check if we already validated it.
# If no, we validate. If it is valid we create a flag to state it is validated to save time in future runs.
# If not valid, we DONT create a flag. Instead we submit a job to create the count_dist, and log it.
def fill_distances(mac_maf, class_name, min_index_to_fill, max_index_to_fill):
    paths_helper = get_paths_helper()
    win_dir = paths_helper.windows_folder + f'{mac_maf}_{class_name}/'
    count_dist_file_template = win_dir + 'count_dist_window_{i}.tsv.gz'
    validated_dir = f'{win_dir}/validated_count_dist/'
    validated_flag_template = validated_dir + '/validated_count_dist_window_{i}.txt'
    os.makedirs(validated_dir, exist_ok=True)

    # this is the log file where we will log any job we submit (and also that we are done.)
    fill_log_file = f'{validated_dir}/fill_log_file_{min_index_to_fill}-{max_index_to_fill}.txt'
    with open(fill_log_file, 'a') as logf:
        logf.write("\n#################################\n")
        logf.write("{:%B %d, %Y %H:%M}".format(datetime.now()))
        logf.write("\n#################################\n")


    num_windows = max_index_to_fill - min_index_to_fill
    counter = 0
    jobs_submitted = 0
    for i in range(min_index_to_fill, max_index_to_fill+1):
        counter +=1
        if counter%100 == 0:
            print(f'done {counter}/{num_windows}')
        count_dist_file = count_dist_file_template.format(i=i)
        validated_flag = validated_flag_template.format(i=i)
        # check if we already validate
        if not os.path.isfile(validated_flag):
            if not _validate_count_dist_file(count_dist_file):
                # delete dist file if exists
                if os.path.isfile(count_dist_file):
                    os.remove(count_dist_file)
                _submit_calc_dist_job(mac_maf, class_name, i)
                jobs_submitted += 1
                _log_job_submitted(fill_log_file, mac_maf, class_name, i)
    with open(fill_log_file, 'a') as logf:
        logf.write(f'Done. {jobs_submitted} jobs submitted.')

def main(args):
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    class_name = args[1]
    min_index_to_fill = int(args[2])
    assert min_index_to_fill >= 0
    max_index_to_fill = int(args[3])
    assert max_index_to_fill > min_index_to_fill

    print('mac_maf',mac_maf)
    print('class_name',class_name)
    print('min_index_to_fill',min_index_to_fill)
    print('max_index_to_fill',max_index_to_fill)

    paths_helper = get_paths_helper()
    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)
    assert max_index_to_fill <= class2num_windows[class_name]

    fill_distances(mac_maf, class_name, min_index_to_fill, max_index_to_fill)

#main(['maf', '0.49', '0', '10'])

if __name__ == "__main__":
   main(sys.argv[1:])