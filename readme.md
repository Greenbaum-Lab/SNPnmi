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
### 2. s2_split_vcfs_by_class (cluster)

2.1 Splits the VCFs by classes.
2.2 collect_split_vcf_stats.py to get stats per class. Mandatory for next steps, and important to understand the data.

### 3. 3_split_to_windows (single run - need to convert to cluster)

Splits each class's indexes randomly to windows.
Because in the next step we read the classes many times (as the number of windows), if we have a big class (for example mac 2 with 73K windows), it is more efficient to generate files with the windows data (and not just the indexes) which we will read in the next step.
So, we have two options: 
1.  big classes(next steps will be more time efficient, but requires a lot of storage):
        There are a few steps to take:
 - generate_windows_and_indexes_files.py
        (takes 35 sec to prepare 2000 sites. We have 7300000/2000 = 3650 * 35 = 127750 / (60*60) = 35 hours)
        OPTION: do this per chr, and merge in the end, this will require a chr param, and a seperate output
        (will take 3.5 minutes to process each window of 100 slices. So about 6 hours for 100 windows. Submitting 400 jobs of 100 each, as we have ~73K windows, we will need to submit twice)
 - split_transposed_windows.py - the previous step generates big windows of 1000. In this step we further split them. 
            TODO - get rid of this step.
            TODO - we should improve so that the output will contain the desired number of sites per window (and not on average).
 - validate_split_transposed_windows
 - submit_transpose_windows.py - the output of the previous step is transposed, so we need to take care of it. TODO - get rid of this step, should be done with the previous.

2. regular size classes: 
 - generate_windows_indexes_files.py
 - validate_windows_indexes.py


### 4. 4_calc_similarity
- submit_calc_dist_windows.py
- fill_calc_distances_in_windows - TODO - currently we need to run this twice before the validation as it generates flags consumed by the validation script, but only if the input exists (otherwise it submits a job to generate it)
-  validate_calc_distances_in_windows.py


 ---
 ---
**CHECKPOINT**
 ---

  By this point what we have is per class a lot of random windows. Based on each we have similarity matrix, not normlized, with the count of sites used in evert entry.

 ---
 ---


### 5. 5_build_baseline_pst
TODO - refactor - many of the code here should also be used in the next step.
- 1_per_class_sum_n_windows.py
- 2_per_class_sum_all_windows
- run validate_all_classes_all_count_dist to validate _per_class_sum_all_windows
Prior to the run of next step you need to manully creare sample_sites_file and indlist_file for netstucrt
- submit_netstruct_per_class.py - will submit a run per class + a run on data from all classes

### 6. TODO - 6_compare_to_random_pst
We will need to build random PST, run onmi and collect results.

# TODO
 -  Add a main script: user interactive, select data and step and params.