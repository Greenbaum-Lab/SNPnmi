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

### cluster
A module which holds wrappers used to execute the various steps on the cluster

### steps
A module which holds scripts used for every processing step

### utils
A module which holds utitlity modules and helpers

### tests
A module  holds tests for the different steps

### notebooks
This is where we store adhoc and dev-test notebooks

### config
This is where we store configuration data for both the cluster enviorment (like root folder) and the different VCFs we use (like name, source, number of individuals etc.)


# Steps

### 1. 1_get_data (single run)

    Gets the VCFS.
    There is also a script to get stats of the VCFs - super useful for understanding the data we have! (some TODOs there, its an old script)
    See the notebook Analyze basic vcftools stats.ipynb.

### 2. 2_split_vcf (cluster)

    Splits the VCFs by classes.
    This is currently an ugly bash script. Need to migrate to python.
    Use the collect_split_vcf_stats.py to get stats per class. Mandatory for next steps, and important to understand the data.


# TODO
 -  Add a main script: user interactive, select data and step and params.