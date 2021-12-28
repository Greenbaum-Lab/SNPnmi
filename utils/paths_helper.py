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
        self.vcf_dir = f'{root_data_folder}vcf/'
        self.data_dir = f'{self.vcf_dir}{dataset_name}/'
        self.checkpoints_folder = f'{self.data_dir}checkpoints/'
        self.vcf_stats_folder = f'{self.data_dir}stats/'
        self.classes_dir = f'{self.data_dir}classes/'
        self.windows_dir = f'{self.classes_dir}windows/'
        self.windows_folder_template = self.windows_dir + '{mac_maf}_{class_name}'
        self.slices_folder = f'{self.classes_dir}slices/'
        self.random_slices_folder = f'{self.classes_dir}random_slices/'
        self.summary_dir = f'{self.classes_dir}summary/'
        self.tree_sizes = self.summary_dir + '/tree_sizes.csv'

        self.net_struct_dir = f'{self.classes_dir}netstruct/'
        self.net_struct_dir_class = f'{self.net_struct_dir}' + '{class_name}/'
        self.net_struct_dir_tree = f'{self.net_struct_dir_class}' + '{class_name}_{tree_hash}'
        self.nmi_dir = f'{self.classes_dir}nmi/'
        self.nmi_class_template = self.nmi_dir + '{class_name}/'
        self.nmi_tree_template = self.nmi_class_template + '{class_name}_{tree_hash}/step_{ns_ss}/'
        self.nmi_file_template = self.nmi_tree_template + '{nmi_type}.txt'
        self.class_by_chr_template = self.classes_dir + '{chr_name}/{class_name}.012'
        self.window_by_class_and_chr_template = self.windows_dir + '{class_name}/{chr_name}/window_{window_id}.012.vcf.gz'
        self.window_by_class_and_chr_np_template = self.windows_dir + '{class_name}/{chr_name}/window_{window_id}.012.npy'
        self.window_by_class_template = self.windows_dir + '{class_name}/window_{window_id}.012.vcf.gz'
        self.windows_indexes_folder = f'{self.windows_dir}indexes/'
        self.windows_indexes_template = self.windows_indexes_folder + '{class_name}/windows_indexes_{chr_name}.pkl'

        # similarity paths
        self.similarity_dir = f'{self.classes_dir}similarity/'
        self.similarity_by_class_folder_template = self.similarity_dir + '{class_name}/'
        self.per_window_similarity = self.similarity_by_class_folder_template + 'per_window_similarity/'
        self.similarity_by_class_and_window_template = self.per_window_similarity + 'similarity_by_window_{window_id}.npy'
        self.count_by_class_and_window_template = self.per_window_similarity + 'count_by_window_{window_id}.npy'
        self.hash_windows_list_template = self.similarity_by_class_folder_template + 'hash_windows_list.json'
        self.hash_winds_lengths_template = self.similarity_by_class_folder_template + 'hash_windows_length.json'

        self.validate_similarity_dir = f'{self.windows_folder_template}/validated_count_similarity/'
        self.validate_similarity_flag_template = self.validate_similarity_dir + '/validated_count_similarity_window_{i}.txt'

        # deprecated
        self.windows_per_class_folder = f'{self.windows_dir}' + '{class_name}/'
        self.number_of_windows_per_class_path = f'{self.windows_dir}number_of_windows_per_class.txt'
        self.number_of_windows_per_class_template = f'{self.windows_dir}' + '{class_name}/number_of_windows.txt'

        # cluster runs logs paths
        self.logs_folder = f'{root_data_folder}logs/'
        self.logs_cluster_folder = f'{self.logs_folder}cluster/'
        self.logs_dataset_folder = f'{self.logs_cluster_folder}{dataset_name}/'
        self.logs_cluster_jobs_stderr_template = self.logs_dataset_folder + '{job_type}/{job_name}.stderr'
        self.logs_cluster_jobs_stdout_template = self.logs_dataset_folder + '{job_type}/{job_name}.stdout'

        self.split_vcf_stats_csv_path = f'{self.logs_dataset_folder}split_vcf_by_class/split_vcf_output_stats.csv'

        # Netstuct inputs paths
        self.netstructh_indlist_path = f'{self.data_dir}{get_indlist_file_name(dataset_name)}'
        self.netstructh_sample_sites_path = f'{self.data_dir}{get_sample_sites_file_name(dataset_name)}'

        # paths to entry points
        self.submit_helper = f'{root_code_folder}{repo_dir_name}/utils/cluster/submit_helper.sh'
        self.wrapper_max_30_params = f'{root_code_folder}{repo_dir_name}/utils/cluster/wrapper_max_30_params.sh'
        self.netstruct_jar = f'{root_code_folder}NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar'
        self.nmi_exe = f'{root_code_folder}Overlapping-NMI/onmi'

        # sanity check folders:
        # TODO -remove?
        self.sanity_check_folder = f'{self.classes_dir}sanity_check/'
        self.sanity_check_dist_folder = f'{self.sanity_check_folder}distances/'
        self.sanity_check_netstruct_folder = f'{self.sanity_check_folder}netstruct/'
        self.sanity_check_nmi_folder = f'{self.sanity_check_folder}nmi/'
