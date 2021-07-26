checkpoint - work on get_vcfs_stats.py

# SNP NMI - using Netstrcut to analyze different classes
(to view in vscode, hit ctrl+k, v)
## Useful links

## Setup
We highly recommend to work in a dedicated virtual env.
To achive that, in Anaconda prompt run:
1. Create an env
``` cmd
conda create --name snpnmi
```
2. Activate env
``` cmd
conda activate snpnmi
```
3. Install pip
``` cmd
conda install pip
```
4. Install requirements
``` cmd
pip install -r requirements.txt
```

### Requirements
Note that the requirements.txt file is used by the built agent and must be up to date.
If you add new dependencies, please include them with the stable version number in the requirements.

## Structure

### runner.py
This script can be used to run a specific step on a specific data set

### steps
A module which holds scripts used for every processing step

### utils
A module which holds utitlity modules and helpers

### tests
A module which holds tests for the different steps

### notebooks
This is where we store adhoc and dev-test notebooks

### config
This is where we store configuration data for both the cluster enviorment (like root folder) and the different VCFs we use (like name, source, number of individuals etc.)


# Steps

### 1. s1_get_data (single run)

 1.1 get_data - gets the VCFs according to the config.

 1.2 get_vcfs_stats - gets VCFs stats. You should atleast look at 'freq' which is fundemental to this work. Others are super useful for understanding the data we have! See the notebook "Analyze basic vcftools stats.ipynb"

 ---

### 2. s2_split_vcfs_by_class (cluster)

2.1 Splits the VCFs by classes.

2.2 collect_split_vcf_stats.py to get stats per class. Mandatory for next steps, and important to understand the data.

---

### 3. 3_split_to_windows (single run - need to convert to cluster)

TODO - validate this in the cluster

1. prepare_for_split_to_windows - Per class run a job(or maybe can run this once?) which builds a mapping of chr to site index to window_index, so that each window size is window_size (or window_size + 1).

TODO - validate this in the cluster

2. Per chr and class we run a job (or maybe more than one?) which writes the sites to a {chr}_{class}_{window_id} file using the mapping from the previous step (TODO - think about the format - currently transposed w.r.t 012)

TODO - validate this in the cluster

3. Per class and max number of window_ids to process in each run, we run a job which collects the files from the previous step to create {class}_{window_id} files (in a 012 gz format)

---

### 4. 4_calc_similarity
- submit_calc_similarity_windows.py
- fill_calc_distances_in_windows - TODO - currently we need to run this twice before the validation as it generates flags consumed by the validation script, but only if the input exists (otherwise it submits a job to generate it)
-  validate_calc_distances_in_windows.py


 ---
 ---
**CHECKPOINT**
 
 ---

  By this point what we have is per class a lot of random windows. Based on each we have similarity matrix, not normlized, with the count of sites used in evert entry.

 ---
 ---

**CLOSING THE LOOP**

1. Run NetstrcutH
- Code - https://github.com/amirubin87/NetStruct_Hierarchy
- old wrapper: submit_netstruct_per_class.py

Cluster:
sbatch --time=72:00:00 --mem=5G --error="C:/Data/HUJI/logs/cluster/hgdp/netstruct_per_class/maf0.49_weighted_true.stderr" --output="C:/Data/HUJI/logs/cluster/hgdp/netstruct_per_class/maf0.49_weighted_true.stdout" --job-name="ns_0.49" C:/Repos/snpnmi/utils/cluster/wrapper_max_30_params.sh 
Job:
java -jar C:/Repos/NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar -ss 0.001 -minb 5 -mino 5 -pro C:/Data/HUJI/vcf/hgdp/classes/netstruct/maf_0.49_all/ -pm C:/Data/HUJI/vcf/hgdp/classes/distances/all_mac_-1--1_maf_1-49_norm_dist.tsv.gz -pmn C:/Data/HUJI/vcf/hgdp/hgdp_wgs.20190516.indlist.csv -pss C:/Data/HUJI/vcf/hgdp/hgdp_wgs.20190516.SampleSites.txt -w false

2. Visualize NetStructH output
- Run Mathematica (when outside HUJI need to use VPN)
- open the notebook:
- in the second cell, change the DataFolder to point to the output of the previous step:
  DataFolder = 
  "C:\\Data\\HUJI\\vcf\\hgdp\\classes\\netstruct\\all_mac_2-18_maf_1-\
49\\W_1_D_0_Min_5_SS_1.0E-4_B_1.0\\";
- Run the cells up to  "tree1". Right click to save the figure to a pdf.

3. Run ONMI to compare resutls
 Get ONMI - https://github.com/aaronmcdaid/Overlapping-NMI and make
 Run it to compare files (leafs only, leafs with all individuals, all nodes)
 Note that the "AllNodes" file needs to be created.. See _collect_all_nodes_if_needed in run_nmi.py.
 example run:
 onmi /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/netstruct/all_mac_-1--1_maf_1-49/W_1_D_0_Min_5_SS_1.0E-4_B_1.0/2_Leafs_NoOverlap.txt /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/netstruct/maf_0.1_all/W_1_D_0_Min_5_SS_0.001_B_1.0/2_Leafs_NoOverlap.txt
 Analyze results like done in: collect NMI.ipynb


## OLD
Need to reveiew the below steps

### 5. 5_build_baseline_pst
TODO - refactor - many of the code here should also be used in the next step.
- 1_per_class_sum_n_windows.py
- 2_per_class_sum_all_windows
- run validate_all_classes_all_count_dist to validate _per_class_sum_all_windows
Prior to the run of next step you need to manully creare sample_sites_file and indlist_file for netstucrt
- submit_netstruct_per_class.py - will submit a run per class + a run on data from all classes

---

### 6. OLD - 6_compare_to_random_pst
We will need to build random PST, run onmi and collect results.
