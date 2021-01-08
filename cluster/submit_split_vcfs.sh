TODO
#!/bin/bash
# this will go over vcfs in the target folde, and will split according to the vcftools params
# params
# data="hgdp"
# output_folder="todo"
# mac_values=2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
# maf_values=1,2,3..,48,49,50
# specific_input="todo" chr22

# first, create the output folder
mkdir output_folder
if specific_input:
    sbatch ../split_vcfs.sh specific_input output_folder mac_values maf_values
else:
    #todo go over vcfs in data
    for vcffile in vcfs:
        sbatch ../split_vcfs.sh vcffile output_folder mac_values maf_values
