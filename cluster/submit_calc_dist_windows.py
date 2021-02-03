import subprocess
import sys
import os

# will submit calc_distances_in_window of given classes and windows

# submit_calc_dist_windows.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/number_of_windows_per_class.txt" 2 2 1 1 49 1 "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stderr/" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stdout/"
def get_number_of_windows_by_class(number_of_windows_per_class_path):
    class2num_windows = dict()
    with open(number_of_windows_per_class_path) as f:
        for l in f.readlines():
            classname, num_windows = l.split(' ',1)
            class2num_windows[classname] = int(num_windows)
    return class2num_windows

def submit_calc_dist_windows(number_of_windows_per_class_path, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, job_stderr_folder, job_stdout_folder):
    os.makedirs(job_stderr_folder, exist_ok=True)
    os.makedirs(job_stdout_folder, exist_ok=True)
    print('go over mac values')
    class2num_windows = get_number_of_windows_by_class(number_of_windows_per_class_path)
    if mac_min_range>0:
        for mac in range(mac_min_range, mac_max_range+1, mac_delta):
            num_windows = class2num_windows[str(mac)]
            print(f'mac {mac}, num_windows {num_windows}')
            for window_id in range(num_windows):
                # go over all windows
                job_stderr_file=f'{job_stderr_folder}mac{mac}_window{window_id}.stderr'
                job_stdout_file=f'{job_stdout_folder}mac{mac}_window{window_id}.stderr'
                job_name=f'c{mac}_w{window_id}'
                cluster_setting=f'sbatch --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                #maf 0.49 0 0.49 0.5 -1 -1
                cmd_to_run=f'{cluster_setting} wrapper_calc_dist_windows.sh mac {mac} {window_id} -1 -1 {mac} {mac}'
                print(cmd_to_run)
                #eval ${cmd_to_run}
    if maf_min_range>0:
        for maf_int in range(maf_min_range, maf_max_range+1, maf_delta):
            maf = f'{maf_int*1.0/100}'
            max_maf = f'{(maf_int + maf_delta)*1.0/100}' 
            num_windows = class2num_windows[maf]
            print(f'maf {maf}, num_windows {num_windows}')
            for window_id in range(num_windows):
                # go over all windows
                job_stderr_file=f'{job_stderr_folder}maf{maf}_window{window_id}.stderr'
                job_stdout_file=f'{job_stdout_folder}maf{maf}_window{window_id}.stderr'
                job_name=f'f{maf}_w{window_id}'
                cluster_setting=f'sbatch --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                #maf 0.49 0 0.49 0.5 -1 -1
                cmd_to_run=f'{cluster_setting} wrapper_calc_dist_windows.sh maf {maf} {window_id} {maf} {max_maf} -1 -1'
                print(cmd_to_run)
                #eval ${cmd_to_run}


submit_calc_dist_windows( r"C:\Data\HUJI\hgdp\classes\number_of_windows_per_class.txt",-1,-1,1,49,49,1,r"/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stderr/", r"/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stdout/")

if __name__ == 'XXX__main__':
    number_of_windows_per_class_path = sys.argv[1]
    # by mac
    mac_min_range = sys.argv[2]
    mac_max_range = sys.argv[3]
    mac_delta = sys.argv[4]

    # by maf
    maf_min_range = sys.argv[5]
    maf_max_range = sys.argv[6]
    maf_delta = sys.argv[7]

    # submission details
    job_stderr_folder = sys.argv[8]
    job_stdout_folder = sys.argv[9]


    # print the inputs
    print('number_of_windows_per_class_path', number_of_windows_per_class_path)
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('mac_delta', mac_delta)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('maf_delta', maf_delta)
    print('job_stderr_folder', job_stderr_folder)
    print('job_stdout_folder', job_stdout_folder)

    submit_calc_dist_windows(number_of_windows_per_class_path, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, job_stderr_folder, job_stdout_folder)