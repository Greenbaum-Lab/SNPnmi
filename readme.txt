The order of scripts to run:
- GetData.sh
- get_vcfs_stats.py
- cluster/submit_split_vcfs.sh
- collect_split_vcf_stats.py
- generate_windows_indexes_files.py
- submit_calc_dist_windows.py (in progress. submitted maf 1-10,15-49)
- TODO - validate submit_calc_dist_windows (done maf 48 49)
- TODO - merge_windows_all / for baseline
- TODO - merge_windows_randomly


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
So, how to do this? If in a class we have N=5K windows(of 100 sites each), we first need to work on groups of sqrt(N), which wil create 50 sub resutls, which we can later merge.
For the largest class, mac=2, we have 73031 windows, we will group every 270 windows, which will results in 271 big-windows, which we can group to the distnaces based on class mac=2.