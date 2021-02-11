The order of scripts to run:
- GetData.sh
- get_vcfs_stats.py
- cluster/submit_split_vcfs.sh
- collect_split_vcf_stats.py
- generate_windows_indexes_files.py
- submit_calc_dist_windows.py (TODO)
- TODO - validate submit_calc_dist_windows

ONMI - from https://github.com/aaronmcdaid/Overlapping-NMI

--------------------------------------
Discussion on submit_calc_dist_windows
--------------------------------------
Processing a window of 100 sites takes 5-8 minutes.
Processing 25 windows will take 3:20 hours at 8 minute per minute
(We will submit with nodes runnign up to 12 hours)
Focusing on maf 0.49, we have 988 windows of 100 sites each.
This requires 40 jobs of 25 windows each.
