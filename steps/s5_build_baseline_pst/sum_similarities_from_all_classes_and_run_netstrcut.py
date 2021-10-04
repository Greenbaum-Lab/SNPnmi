# will use all classes distances matrixes to create a big matrix with all data
# takes about 1 minute to group 66 windows
# python3 3_sum_distances_from_all_classes.py 2 18 1 49

# the commands to use to run netstruct on the result:
# mkdir /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/
# sbatch --time=72:00:00 --mem=5G --error="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stderr" --output="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stdout" --job-name="s3_nt_a" /cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh java -jar /cs/icore/amir.rubin2/code/NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar -ss 0.001 -minb 5 -mino 5 -pro /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/ -pm /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/mac_2-18_maf_1-49_windows_0-499_norm_dist.tsv.gz -pmn /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.indlist.csv -pss /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.SampleSites.txt -w true
import gzip
import time
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer, Loader
from utils.common import get_paths_helper, args_parser, are_running_submitions, validate_stderr_empty
from utils.similarity_helper import generate_similarity_matrix, numpy_to_file012, matrix_to_edges_file
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

    class_range_str = f'all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}'
    output_file_name = paths_helper.similarity_dir + class_range_str
    generate_similarity_matrix(similarity_files, count_files, paths_helper.similarity_dir, output_file_name)
    return output_file_name, class_range_str


def convert_numpy_array_to_lists(similarity_matrix_path):
    lists = numpy_to_file012(similarity_matrix_path)
    output_path = similarity_matrix_path[:-3] + 'vcf.gz'
    with gzip.open(output_path, 'wb') as old_format:
        old_format.write(lists.encode())
    return output_path


def main(options):
    mac_min_range, mac_max_range = options.mac if options.mac else (0, 0)
    maf_min_range, maf_max_range = options.maf if options.maf else (0, 0)

    output_files_name, all_class_range_str = sum_all_classes(options)
    print(output_files_name)

    paths_helper = get_paths_helper(options.dataset_name)
    job_type = 'netstruct'
    job_long_name = f'netstruct_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}'
    job_name = f'ns_{mac_min_range}-{mac_max_range}_{maf_min_range}-{maf_max_range}'
    similarity_matrix_path = output_files_name + '_similarity.npy'
    similarity_edges_file = output_files_name + '_edges.txt'
    matrix_to_edges_file(similarity_matrix_path, similarity_edges_file)
    output_folder = paths_helper.net_struct_dir + all_class_range_str + '/'
    print(output_folder)
    err_file = submit_netstcut(options, job_type, job_long_name, job_name, similarity_edges_file, output_folder, netstrcut_ss=0.005)

    while are_running_submitions(string_to_find="ns"):
        with Loader("Running NetStruct_Hierarchy"):
            time.sleep(5)

    if not err_file:
        return False

    assert validate_stderr_empty([err_file])
    return True


if __name__ == "__main__":
    arguments = args_parser()
    with Timer(f"run net-struct with {arguments}"):
        main(arguments)
