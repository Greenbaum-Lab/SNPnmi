import subprocess
import sys
import os

# will submit calc_distances_in_window of given classes and windows

# python3 submit_calc_dist_windows.py "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/number_of_windows_per_class.txt" 2 2 1 1 49 1 "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stderr/" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stdout/" 10 r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/'
def get_number_of_windows_by_class(number_of_windows_per_class_path):
    class2num_windows = dict()
    with open(number_of_windows_per_class_path) as f:
        for l in f.readlines():
            classname, num_windows = l.split(' ',1)
            class2num_windows[classname] = int(num_windows)
    return class2num_windows

def submit_calc_dist_windows(number_of_windows_per_job, number_of_windows_per_class_path, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, job_stderr_folder, job_stdout_folder, classes_folder):
    os.makedirs(job_stderr_folder, exist_ok=True)
    os.makedirs(job_stdout_folder, exist_ok=True)

    print('go over mac values')
    class2num_windows = get_number_of_windows_by_class(number_of_windows_per_class_path)
    if mac_min_range>0:
        for mac in range(mac_min_range, mac_max_range+1, mac_delta):
            num_windows = class2num_windows[str(mac)]
            print(f'mac {mac}, num_windows {num_windows}')
            window_id = 0
            while window_id <= num_windows:
                min_window_id = window_id
                max_window_id = min(min_window_id + number_of_windows_per_job, num_windows)
                window_id = max_window_id + 1
                # go over all windows
                job_stderr_file=f'{job_stderr_folder}mac{mac}_windows{min_window_id}-{max_window_id}.stderr'
                job_stdout_file=f'{job_stdout_folder}mac{mac}_windows{min_window_id}-{max_window_id}.stdout'
                job_name=f'c{mac}_w{window_id}'
                cluster_setting=f'sbatch --time=16:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                cmd_to_run=f'{cluster_setting} /cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_calc_dist_windows.sh mac {mac} {min_window_id} {max_window_id} -1 -1 {mac} {mac} {classes_folder}'
                print(cmd_to_run)
                #subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
    if maf_min_range>0:
        for maf_int in range(maf_min_range, maf_max_range+1, maf_delta):
            maf = f'{maf_int*1.0/100}'
            max_maf = f'{(maf_int + maf_delta)*1.0/100}' 
            num_windows = class2num_windows[maf]
            print(f'maf {maf}, num_windows {num_windows}')
            window_id = 0
            while window_id <= num_windows:
                min_window_id = window_id
                max_window_id = min(min_window_id + number_of_windows_per_job, num_windows)
                window_id = max_window_id + 1
                # go over all windows
                job_stderr_file=f'{job_stderr_folder}maf{maf}_windows{min_window_id}-{max_window_id}.stderr'
                job_stdout_file=f'{job_stdout_folder}maf{maf}_windows{min_window_id}-{max_window_id}.stdout'
                job_name=f'f{maf}_w{window_id}'
                cluster_setting=f'sbatch'# --error="{job_stderr_file}"'# --output="{job_stdout_file}" --job-name="{job_name}"'
                #maf 0.49 0 0.49 0.5 -1 -1
                cmd_to_run=f'{cluster_setting} /cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_calc_dist_windows.sh maf {maf} {min_window_id} {max_window_id} {maf} {max_maf} -1 -1 {classes_folder}'
                print(cmd_to_run)
                #subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])

#submit_calc_dist_windows(r"/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/number_of_windows_per_class.txt",-1,-1,1,49,49,1,r"/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stderr/", r"/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stdout/", 1000)

if __name__ == '__main__':
    number_of_windows_per_class_path = sys.argv[1]
    # by mac
    mac_min_range = int(sys.argv[2])
    mac_max_range = int(sys.argv[3])
    mac_delta = int(sys.argv[4])

    # by maf
    maf_min_range = int(sys.argv[5])
    maf_max_range = int(sys.argv[6])
    maf_delta = int(sys.argv[7])

    # submission details
    job_stderr_folder = sys.argv[8]
    job_stdout_folder = sys.argv[9]
    number_of_windows_per_job =  int(sys.argv[10])
    classes_folder = sys.argv[11]

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
    print('number_of_windows_per_job', number_of_windows_per_job)
    print('classes_folder', classes_folder)

    submit_calc_dist_windows(number_of_windows_per_job, number_of_windows_per_class_path, mac_min_range, mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, job_stderr_folder, job_stdout_folder, classes_folder)
