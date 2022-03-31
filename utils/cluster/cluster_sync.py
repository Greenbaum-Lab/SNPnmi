import subprocess
import sys
import time
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader
from utils.common import warp_how_many_jobs


def sync_dir(source_list):
    num_hours_to_run = 8
    memory = 8
    for source in source_list:
        job_stderr_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_err_files/{source}.err'
        job_stdout_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_out_files/{source}.out'
        job_name = 'dr' + source[-3:]
        cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --error="{job_stderr_file}' \
                          f'" --output="{job_stdout_file}" --job-name="{job_name}"'
        cmd_line = f'rclone sync /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source} remote:gili_lab/vcf/{source}'
        warp_30_params_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/wrapper_max_30_params.sh'
        submit_helper_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/submit_helper.sh'
        subprocess.run([submit_helper_path, f'{cluster_setting} {warp_30_params_path} {cmd_line}'])

    jobs_func = warp_how_many_jobs("dr")
    with Loader(f"uploading dir {dirname(source_list[0])}", jobs_func):
        while jobs_func():
            time.sleep(5)
    print("Done sync!")


def del_dir(source_list):
    warp_30_params_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/wrapper_max_30_params.sh'
    submit_helper_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/submit_helper.sh'
    num_hours_to_run = 2
    memory = 8
    for source in source_list:
        delete_line = f'rm -rf /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source}'
        job_name = 'rm' + source[-3:]
        cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --job-name={job_name}'
        subprocess.run([submit_helper_path, f'{cluster_setting} {warp_30_params_path} {delete_line}'])
    jobs_func = warp_how_many_jobs("rm")
    with Loader(f"deleting {dirname(dirname(source_list[0]))}", jobs_func):
        while jobs_func():
            time.sleep(5)


def tar_files(source_list):
    warp_30_params_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/wrapper_max_30_params.sh'
    submit_helper_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/submit_helper.sh'
    num_hours_to_run = 24
    memory = 8
    for source in source_list:
        job_stderr_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_err_files/{source}.err'
        job_stdout_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_out_files/{source}.err'
        tar_line = f'tar -czf /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source}.tar.gz /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source}'
        job_name = 'tr' + source[-3:]
        cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --job-name={job_name} --error="{job_stderr_file}"'
        subprocess.run([submit_helper_path, f'{cluster_setting} {warp_30_params_path} {tar_line}'])
    jobs_func = warp_how_many_jobs("tr")
    with Loader(f"Taring {dirname(source_list[0])}", jobs_func):
        while jobs_func():
            time.sleep(5)


mac_lst = [f'arabidopsis/classes/similarity/mac_{i}' for i in range(2, 71)]
maf_lst = [f'arabidopsis/classes/similarity/maf_{i/100}' for i in range(1, 50)]
tmp = [f'arabidopsis/classes/similarity/mac_{i}' for i in [2,4]]
lst = mac_lst + maf_lst
# sync_dir(source_list)
tar_files(tmp)
