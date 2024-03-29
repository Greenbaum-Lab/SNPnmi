import sys
import os
from os.path import dirname, abspath

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_paths_helper, hash_args, str_for_timer
from datetime import datetime

def get_checkpoint_file_path(dataset_name, checkpoint_name, options):
    paths_helper = get_paths_helper(dataset_name)
    checkpoint_folder = paths_helper.checkpoints_folder
    args_hash = str(hash_args(options))
    return f'{checkpoint_folder}{checkpoint_name}.{args_hash}.checkpoint'

def write_checkpoint_file(checkpoint_file_path, args):
    os.makedirs(dirname(checkpoint_file_path), exist_ok=True)
    with open(checkpoint_file_path, 'w') as checkpoint_file:
        checkpoint_file.write(str(datetime.utcnow()) +'\n')
        checkpoint_file.write(f'args:{args}')
    print(f'checkpoint created: {checkpoint_file_path}')

def get_checkpoint_time(checkpoint_file_path):
    if os.path.isfile(checkpoint_file_path):
        with open(checkpoint_file_path) as checkpoint_file:
            return checkpoint_file.readline()
    return None

def checkpoint_file_check(dataset_name, checkpoint_name, options):
    checkpoint_file = get_checkpoint_file_path(dataset_name, checkpoint_name, options)
    checkpoint_file_exist = os.path.exists(checkpoint_file)
    if checkpoint_file_exist:
        checkpoint_time = get_checkpoint_time(checkpoint_file)
        print(f'Checkpoint exists from {checkpoint_time}. ({checkpoint_file}) Break.')
    return checkpoint_file_exist, checkpoint_file


# require that:
# 1. the first argument in args is dataset_name
# 2. func accepts args and returns bool
def execute_with_checkpoint(func, checkpoint_name, options):
    is_checkpoint_exists, checkpoint_file = checkpoint_file_check(options.dataset_name, checkpoint_name, options)
    if is_checkpoint_exists:
        return True, 'checkpoint found'

    success = func(options)
    if success:
        write_checkpoint_file(checkpoint_file, options.args)
        return True, 'successful run'
    return False, 'non successful run'