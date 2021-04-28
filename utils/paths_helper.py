import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.config import *

repo_dir_name = 'snpnmi'
class PathsHelper:
    # example: data_folder="/vol/sci/bio/data/gil.greenbaum/amir.rubin/", dataset_name='hgdp'
    def __init__(self, root_data_folder: str, root_code_folder: str, dataset_name: str):
        # data main paths
        self.vcf_folder = f'{root_data_folder}vcf/'
        self.data_folder = f'{self.vcf_folder}{dataset_name}/'
        self.checkpoints_folder = f'{self.data_folder}checkpoints/'
        self.vcf_stats_folder = f'{self.data_folder}stats/'
        self.classes_folder = f'{self.data_folder}classes/'
        self.windows_folder = f'{self.classes_folder}windows/'
        self.slices_folder = f'{self.classes_folder}slices/'
        self.random_slices_folder = f'{self.classes_folder}random_slices/'

        # TODO - similarity?
        self.dist_folder = f'{self.classes_folder}distances/'
        self.netstruct_folder = f'{self.classes_folder}netstruct/'
        self.nmi_folder = f'{self.classes_folder}nmi/'

        self.windows_indexes_folder = f'{self.windows_folder}indexes/'
        self.windows_indexes_template = self.windows_indexes_folder + 'windows_indexes_for_class_{class_name}.json'

        self.count_dist_window_template = self.windows_folder + '{mac_maf}_{class_name}/count_dist_window_{window_index}.tsv.gz'

        self.number_of_windows_per_class_path = f'{self.windows_indexes_folder}number_of_windows_per_class.txt'

        # cluster runs logs paths
        self.logs_folder = f'{root_data_folder}logs/'
        self.logs_cluster_folder = f'{self.logs_folder}cluster/'
        self.logs_cluster_jobs_stderr_template = self.logs_cluster_folder + '{job_type}/{job_name}.stderr'
        self.logs_cluster_jobs_stdout_template = self.logs_cluster_folder + '{job_type}/{job_name}.stdout'

        self.split_vcf_stats_csv_path = f'{self.logs_cluster_folder}split_vcfs/split_vcf_output_stats.csv'

        # Netstuct inputs paths
        self.netstructh_indlist_path = f'{self.data_folder}{get_indlist_file_name()}'
        self.netstructh_sample_sites_path = f'{self.data_folder}{get_sample_sites_file_name()}'

        # paths to entry points
        self.submit_helper = f'{root_code_folder}{repo_dir_name}/utils/cluster/submit_helper.sh'
        self.wrapper_max_30_params = f'{root_code_folder}{repo_dir_name}/utils/cluster/wrapper_max_30_params.sh'
        self.netstruct_jar = f'{root_code_folder}NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar'
        self.nmi_exe = f'{root_code_folder}Overlapping-NMI/onmi'

        # sanity check folders:
        # TODO -remove?
        self.sanity_check_folder = f'{self.classes_folder}sanity_check/'
        self.sanity_check_dist_folder = f'{self.sanity_check_folder}distances/'
        self.sanity_check_netstruct_folder = f'{self.sanity_check_folder}netstruct/'
        self.sanity_check_nmi_folder = f'{self.sanity_check_folder}nmi/'
