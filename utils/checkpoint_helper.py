import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_paths_helper
from datetime import datetime

def get_checkpoint_file_path(dataset_name, script_name):
    paths_helper = get_paths_helper(dataset_name)
    checkpoint_folder = paths_helper.checkpoints_folder
    return f'{checkpoint_folder}{script_name}.checkpoint'

def write_checkpoint_file(checkpoint_file_path):
    os.makedirs(dirname(checkpoint_file_path), exist_ok=True)
    with open(checkpoint_file_path, 'w') as checkpoint_file:
        checkpoint_file.write(str(datetime.utcnow()))
    print(f'checkpoint created: {checkpoint_file_path}')

def get_checkpoint_time(checkpoint_file_path):
    if os.path.isfile(checkpoint_file_path):
        with open(checkpoint_file_path) as checkpoint_file:
            return checkpoint_file.readline()
    return None