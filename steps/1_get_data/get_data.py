# TODO - this is the first script written. Need to go over it, see it can be used for other VCFs.
import urllib.request
import logging
import argparse
import sys
from utils.config import *
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper

# TODO - I gave up on the logger in other scripts, print works fine
logger = logging.getLogger(__name__)

# TODO - I gave up on the parser in other scripts.
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

    paths_helper = get_paths_helper()

    local_data_folder = paths_helper.data_folder()
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


'''
The following was used to compare the sized of source to those on the cluster:

sizes_cluster = "C:\Data\HUJI\hgdp\sizes_cluster.tsv"
sizes_source = "C:\Data\HUJI\hgdp\sizes_source.tsv"
import pandas as pd
source = pd.read_csv(sizes_source, sep='\t', header=None, names =  ['key','size2'])
source['size2'] = source['size2'].str.replace(' KB', '')
source.head()
cluster = pd.read_csv(sizes_cluster, sep=' ', header=None, names =  ['size1','1','2','3','4','key'])
cluster['size1'] = cluster['size1'].str.replace('K', '')

cluster.head()
joined = cluster.join(source.set_index('key'), on='key')
joined = joined[joined['key'].str.endswith('.gz')]
joined[joined['size1']!=joined['size2']]

'''