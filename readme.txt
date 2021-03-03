The order of scripts to run:
- GetData.sh
- get_vcfs_stats.py
- cluster/submit_split_vcfs.sh
- collect_split_vcf_stats.py
- select: 
-  big classes(next steps will be more time efficient, but requires a lot of storage): generate_windows_indexes_files.py
 (will take 3.5 minutes to process each window of 100 slices. So about 6 hours for 100 windows. Submitting 400 jobs of 100 each, as we have ~73K windows, we will need to submit twice)
- regular size classes: generate_windows_and_indexes_files.py
- submit_calc_dist_windows.py (in progress. submitted mac 4-18, maf 1-49) ** this takes a long time and a lot of jobs **
- TODO - many missing from previous step!!!
- TODO - validate_calc_distances_in_windows (takes ~1 minute for 1K windows)
- TODO - submit_merge_windows (is_random=False to join all for baseline) (takes ~13 minutes to process 1K windows)
- TODO - merge_slices_to_normalized_dist_matrix (to generate ground truth)
- TODO - submit_merge_windows (is_random=True to generate random slices)
- TODO - run_netstruct
- TODO - calc ONMI
- TODO - collect ONMI


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