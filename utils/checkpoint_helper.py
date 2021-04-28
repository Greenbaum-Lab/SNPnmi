import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_paths_helper
from datetime import datetime

def get_checkpoint_file_path(dataset_name, script_name, args):
    paths_helper = get_paths_helper(dataset_name)
    checkpoint_folder = paths_helper.checkpoints_folder
    args_hash = str(hash(frozenset(args)))
    return f'{checkpoint_folder}{script_name}.{args_hash}.checkpoint'

def write_checkpoint_file(checkpoint_file_path, args):
    os.makedirs(dirname(checkpoint_file_path), exist_ok=True)
    with open(checkpoint_file_path, 'w') as checkpoint_file:
        checkpoint_file.write(str(datetime.utcnow()) +'\n')
        checkpoint_file.write(f'args:{",".join(args)}')
    print(f'checkpoint created: {checkpoint_file_path}')

def get_checkpoint_time(checkpoint_file_path):
    if os.path.isfile(checkpoint_file_path):
        with open(checkpoint_file_path) as checkpoint_file:
            return checkpoint_file.readline()
    return None

# require that:
# 1. the first argument in args is dataset_name (#TODO add validation for that)
# 2. func accepts args and returns bool
def execute_with_checkpoint(func, args):
    dataset_name = args[0]
    chekpoint_file = get_checkpoint_file_path(dataset_name, os.path.basename(__file__), args)
    if os.path.exists(chekpoint_file):
        checkpoint_time = get_checkpoint_time(chekpoint_file, args)
        print (f'Checkpoint exists from {checkpoint_time} ({chekpoint_file}). Break.')
        return 'checkpoint found'

    success = func(args)
    if success:
        write_checkpoint_file(chekpoint_file, args)
    return 'done'