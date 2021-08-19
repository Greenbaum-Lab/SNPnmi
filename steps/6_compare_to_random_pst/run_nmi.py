# Run nmi on all outputs of netstruct w.r.t ground truth
# python3 4_run_nmi.py 2 18 1 49 0 499 mac_2-18_maf_1-49_windows_0-499 W_1_D_0_Min_5_SS_0.001_B_1.0

DEBUG=False

# paths 
# /code/Overlapping-NMI/nmi
# /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_9_0-499/W_1_D_0_Min_5_SS_0.001_B_1.0/2_Leafs_WithOverlap.txt
# /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499

import os
import subprocess
import sys
import time
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_paths_helper

# TODO extract to nmi_helper
def _run_nmi(input1, input2, output_path):
    paths_helper = get_paths_helper()
    nmi_path = paths_helper.nmi_exe
    nmi_process = subprocess.Popen((nmi_path, input1, input2), stdout=subprocess.PIPE)
    nmi_output = nmi_process.stdout.read().decode()
    with open(output_path, 'w') as f:
        f.write(nmi_output)

    with open(output_path) as f:
        for line in f:
            print(line)

    if DEBUG:
        print(input1, input2, output_path)
        print(nmi_output)

def _collect_all_nodes_if_needed(folder):
    #only if output file does not exist we will create it
    all_nodes_path = f'{folder}AllNodes.txt'
    if os.path.exists(all_nodes_path):
        print(f'All nodes exists: {all_nodes_path}')
        return all_nodes_path

    # look for community files in the folder, append them to the all_nodes_path file
    with open(all_nodes_path, 'a') as all_nodes_file:
        for f in os.listdir(folder):
            if f.endswith('_C.txt'):
                with open(folder+f) as comm_file:
                    all_nodes_file.write(comm_file.read())
    return all_nodes_path


def run_nmi_on_all(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, ground_truth, netstruct_folder_name):
    # prepare paths
    paths_helper = get_paths_helper()
    netstruct_base_folder = paths_helper.net_struct_dir
    nmi_output_folder = paths_helper.nmi_folder

    os.makedirs(nmi_output_folder, exist_ok=True)

    # ground truth files
    ground_truth_folder = f'{netstruct_base_folder}{ground_truth}/{netstruct_folder_name}/'
    ground_truth_leafs_overlap = f'{ground_truth_folder}2_Leafs_WithOverlap.txt'
    ground_truth_leafs_no_overlap = f'{ground_truth_folder}2_Leafs_NoOverlap.txt'
    # first, we need to create a set of all levels of the tree
    ground_truth_all_nodes = _collect_all_nodes_if_needed(ground_truth_folder)

    # go over classes
    for mac_maf in ['mac', 'maf']:
            is_mac = mac_maf == 'mac'
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            if min_range>0:
                print(f'go over {mac_maf} values: [{min_range},{max_range}]')
                for val in range(min_range, max_range+1):
                    # in maf we take 0.x
                    if not is_mac:
                        val = f'{val * 1.0/100}'
                    print(val)
                    # input template
                    class_netstuct_folder = f'{netstruct_base_folder}{mac_maf}_{val}_{min_window_index}-{max_window_index}/{netstruct_folder_name}/'
                    class_leafs_overlap = f'{class_netstuct_folder}2_Leafs_WithOverlap.txt'
                    class_leafs_no_overlap = f'{class_netstuct_folder}2_Leafs_NoOverlap.txt'
                    # first, we need to create a set of all levels of the tree
                    class_all_nodes = _collect_all_nodes_if_needed(class_netstuct_folder)
                    class_nmi_output = f'{nmi_output_folder}{mac_maf}_{val}_{min_window_index}-{max_window_index}_nmi_'

                    # calc nmi
                    _run_nmi(ground_truth_leafs_overlap, class_leafs_overlap, class_nmi_output + 'Leaves_WithOverlap.txt')

                    _run_nmi(ground_truth_leafs_no_overlap, class_leafs_no_overlap, class_nmi_output + 'Leaves_NoOverlap.txt')

                    _run_nmi(ground_truth_all_nodes, class_all_nodes, class_nmi_output + 'AllNodes.txt')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    # by mac
    mac_min_range = int(args[0])
    mac_max_range = int(args[1])

    # by maf
    maf_min_range = int(args[2])
    maf_max_range = int(args[3])

    # windows used
    min_window_index =  int(args[4])
    max_window_index =  int(args[5])

    ground_truth =  args[6]
    netstruct_folder_name = args[7]

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)
    print('ground_truth', ground_truth)
    print('netstruct_folder_name', netstruct_folder_name)

    run_nmi_on_all(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, ground_truth, netstruct_folder_name)

    print(f'{(time.time()-s)/60} minutes total run time')

#main([2, 4, 100, 49, 0, 499, 'mac_2-18_maf_1-49_windows_0-499', 'W_1_D_0_Min_5_SS_0.001_B_1.0', ])

if __name__ == "__main__":
   main(sys.argv[1:])