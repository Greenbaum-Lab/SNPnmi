import urllib.request
import logging
import argparse
import sys
from utils.config import *
import os

logger = logging.getLogger(__name__)

def init_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-name', type=str, dest='data_name', default='hgdp', help='the name of the dataset to work with')
    return parser

def main(in_args):
    parser = init_parser()
    args = parser.parse_args(in_args)
    data_name = args.data_name

    config_data = get_config(CONFIG_NAME_DATA)
    source = config_data[data_name]['source']
    files_names = config_data[data_name]['files_names']

    local_data_folder = get_local_data_folder(data_name)
    logger.info('output folder is ' + local_data_folder)
    os.makedirs(local_data_folder, exist_ok=True)

    for f in files_names:
        dest = local_data_folder + f
        if os.path.exists(dest):
            logger.info('File exists in ' + dest)
        else:
            logger.info('Downloading ' + f)
            urllib.request.urlretrieve(source + f, dest)



if __name__ == '__main__':
    main(sys.argv[1:])