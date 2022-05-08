#!/usr/bin/python3
import subprocess
from os.path import dirname, abspath, basename
import sys


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


from utils.common import args_parser


def sync_cluster_to_google_drive(source_file, dest_dir, dest_file):
    if dest_file:
        subprocess.run(['rclone', 'sync', source_file, dest_dir + dest_file])
    else:
        subprocess.run(['rclone', 'sync', source_file, dest_dir])

    output = subprocess.run(['rclone', 'check', source_file, dest_dir], check=True, capture_output=True)
    error = 'ERROR' in output.stderr.decode()
    assert not error, f"{source_file} was not found in {dest_dir}. Sync to google drive failed!"


def main(options):
    dest_file = options.args[2] if len(options.args) == 3 else None
    sync_cluster_to_google_drive(options.args[0], options.args[1], dest_file)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
