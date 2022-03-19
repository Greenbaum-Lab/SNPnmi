import subprocess
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


def sync_dir(source):
    num_hours_to_run = 2
    memory = 8
    job_stderr_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_err_files/{source}'
    job_stdout_file = f'/sci/labs/gilig/shahar.mazie/icore-data/tmp/cluster_out_files/{source}'
    job_name = source[:-5]
    cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --error="{job_stderr_file}' \
                      f'" --output="{job_stdout_file}" --job-name="{job_name}"'
    cmd_line = f'rclone copy /sci/labs/gilig/shahar.mazie/icore-data/vcf/{source} remote:gili_lab/vcf/{source}'
    warp_30_params_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/wrapper_max_30_params.sh'
    submit_helper_path = '/sci/labs/gilig/shahar.mazie/icore-data/code/snpnmi/utils/cluster/submit_helper.sh'
    print('Start!')
    subprocess.run([submit_helper_path, f'{cluster_setting} {warp_30_params_path} {cmd_line}'])
    print('Done!')

sync_dir('test_file.txt')
