import subprocess
import sys
import time
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader
from utils.common import how_many_jobs_run

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

    with Loader(f"uploading dir {dirname(source_list[0])}", string_to_find="dr"):
        while how_many_jobs_run(string_to_find="dr"):
            time.sleep(5)
    print("Done sync!")


def del_dir(source_list):
    warp_30_params_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/wrapper_max_30_params.sh'
    submit_helper_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/submit_helper.sh'
    num_hours_to_run = 8
    memory = 8
    for source in source_list:
        job_stderr_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_err_files/rm_{source}'
        job_stdout_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_out_files/rm_{source}'
        delete_line = f'rm -rf /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source}'
        job_name = 'rm' + source[:-3]
        cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --job-name={job_name}'
        subprocess.run([submit_helper_path, f'{cluster_setting} {warp_30_params_path} {delete_line}'])
    with Loader(f"uploading {dirname(dirname(source_list[0]))}", string_to_find="rm"):
        while how_many_jobs_run(string_to_find="rm"):
            time.sleep(5)


source_list = []
for i in range(23, 50):
    maf = i / 100
    mini_source_list = [f'hgdp/classes/windows/maf_{maf}/chr'] * 22
    for j in range(1, 23):
        mini_source_list[j-1] += str(j)
    source_list += mini_source_list

# sync_dir(source_list)
del_dir(source_list)

