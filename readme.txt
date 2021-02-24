The order of scripts to run:
- GetData.sh
- get_vcfs_stats.py
- cluster/submit_split_vcfs.sh
- collect_split_vcf_stats.py
- generate_windows_indexes_files.py
- submit_calc_dist_windows.py (in progress. submitted mac 4-18, maf 1-49) ** this takes a long time and a lot of jobs **
- TODO - validate_calc_distances_in_windows (done maf 48 49)
- TODO - submit_merge_windows (is_random=False to join all for baseline)
- TODO - submit_merge_windows (is_random=True to generate random slices)


ONMI - from https://github.com/aaronmcdaid/Overlapping-NMI

--------------------------------------
Discussion on submit_calc_dist_windows
--------------------------------------
Processing a window of 100 sites takes 5-8 minutes.
Processing 25 windows will take 3:20 hours at 8 minute per minute
(We will submit with nodes runnign up to 12 hours)
Focusing on maf 0.49, we have 988 windows of 100 sites each.
This requires 40 jobs of 25 windows each.


--------------------------------------
Discussion on building base line
--------------------------------------
We will build a baseline per class.
Then we can easily merge all classes to one.
Seems like merging 1000 windows takes about 7 minutes, so we will simply merge 1000 windows at a job.
For the biggest class, mac 2, we have 73K windows, so only 73 jobs.