# will use all classes distances matrixes to create a big matrix with all data
# takes about 1 minute to group 66 windows
# python3 3_sum_distances_from_all_classes.py 2 18 1 49

# the commands to use to run netstruct on the result:
# mkdir /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/
# sbatch --time=72:00:00 --mem=5G --error="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stderr" --output="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stdout" --job-name="s3_nt_a" /cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh java -jar /cs/icore/amir.rubin2/code/NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar -ss 0.001 -minb 5 -mino 5 -pro /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/ -pm /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/mac_2-18_maf_1-49_windows_0-499_norm_dist.tsv.gz -pmn /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.indlist.csv -pss /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.SampleSites.txt -w true


import time
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper, str2bool, args_parser
from utils.similarity_helper import generate_similarity_matrix
from utils.netstrcut_helper import submit_netstcut


def sum_all_classes(options):
    mac_min_range, mac_max_range = options.mac if options.mac else (0, 0)
    maf_min_range, maf_max_range = options.maf if options.maf else (0, 0)
    paths_helper = get_paths_helper(options.dataset_name)
    # get inputs
    similarity_files = []
    count_files = []
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_str = f"{mac_maf}_{val}"
                similarity_file = paths_helper.similarity_by_class_folder_template.format(class_name=class_str) + \
                                  f"{class_str}_all_similarity.npy"
                count_file = paths_helper.similarity_by_class_folder_template.format(class_name=class_str) + \
                             f"{class_str}_all_count.npy"
                similarity_files.append(similarity_file)
                count_files.append(count_file)

    output_files_name = paths_helper.similarity_folder + f'all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}'
    generate_similarity_matrix(similarity_files, count_files, paths_helper.similarity_folder, output_files_name)
    return output_files_name


def main(options):
    mac_min_range, mac_max_range = options.mac if options.mac else (0, 0)
    maf_min_range, maf_max_range = options.maf if options.maf else (0, 0)

    output_files_name = sum_all_classes(options)
    print(output_files_name)
    if False:
        paths_helper = get_paths_helper(options.dataset_name)
        job_type = 'netstruct_on_classes'
        job_long_name = f'netstruct_on_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}'
        job_name = 'ns_{mac_min_range}-{mac_max_range}_{maf_min_range}-{maf_max_range}'
        similarity_matrix_path = paths_helper.dist_folder + output_files_name + '_norm_dist.tsv.gz'
        output_folder = paths_helper.netstruct_folder + output_files_name + '/'
        print(output_folder)
        submit_netstcut(job_type, job_long_name, job_name, similarity_matrix_path, output_folder, netstrcut_ss=0.0005)


if __name__ == "__main__":
    arguments = args_parser()
    with Timer(f"run net-struct with {arguments}"):
        main(arguments)
