#!/usr/bin/python3
import subprocess
from os.path import dirname, abspath, basename
import sys


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


from utils.common import args_parser


def sync_cluster_to_google_drive(options, source_file, dest_file):
    subprocess.run(['rclone', 'sync', source_file, dest_file])
    subprocess.run(['rclone', 'check', source_file, dest_file])


def main(options):
    sync_cluster_to_google_drive(options, options.args[0], options.args[1])


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
