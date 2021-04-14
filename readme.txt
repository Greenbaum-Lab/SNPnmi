Bookmark:
 RUNNING - 1_per_class_sum_n_windows.py
 NEXT - validate the above and run 2_per_class_sum_all_windows.py (need to check it is ok before running)
 Done -  fill_calc_distances_in_windows on all classes with windows of 1000.


NetStruct cmd
    java -jar /cs/icore/amir.rubin2/code/NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar -ss 0.001 -dy false -mod true -minb 5 -mino 5 -b 1.0 -pro /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-19_maf_1-49_0-499/ -skip false -pm /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/maf_0.49_0-499_norm_dist.tsv.gz -pmn /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.indlist.csv -pss /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.SampleSites.txt -nvl 1 -w true

------------------------------------------
The order of scripts to run:
------------------------------------------
- GetData.sh
- get_vcfs_stats.py
- cluster/submit_split_vcfs.sh
- collect_split_vcf_stats.py
- one of the below: 
    1.  big classes(next steps will be more time efficient, but requires a lot of storage): 
        - generate_windows_and_indexes_files.py
        (takes 35 sec to prepare 2000 sites. We have 7300000/2000 = 3650 * 35 = 127750 / (60*60) = 35 hours)
        OPTION: do this per chr, and merge in the end, this will require a chr param, and a seperate output
        (will take 3.5 minutes to process each window of 100 slices. So about 6 hours for 100 windows. Submitting 400 jobs of 100 each, as we have ~73K windows, we will need to submit twice)
        - split_transposed_windows.py - seems like this is done
        - validate the above (validate_split_transposed_windows) running in screen 19915.fillmac2
        - transpose_files - done 73512 ( +2 dirs old_count_dist, transposed)
        - validae - in mac 2 we have 7303147 sites (which is what we should have according to the split_vcf_output_stats file), in 73512 012 files.
        - Done (cluster) 1000/73031: calc_distances_in_window using "python3 submit_calc_dist_windows.py 2 2 1 100 50 1 -1 -1 -1 True 0 73031"
    2. regular size classes: generate_windows_indexes_files.py 
- submit_calc_dist_windows.py
- fill_calc_distances_in_windows - run this twice before the validation as it generates flags consumed by the validation script
-  validate_calc_distances_in_windows.py
--------------------
Build baseline
- submit_1_per_class_sum_n_windows.py aggregating 1000 windows
- TODO aggregate all windows per class
- 3_sum_distances_from_all_classes.py
--------------------

- TODO - submit_merge_windows (is_random=False to join all for baseline) (takes ~13 minutes to process 1K windows)
- TODO - merge_slices_to_normalized_dist_matrix (to generate ground truth)
- TODO - submit_merge_windows (is_random=True to generate random slices)
- TODO - run_netstruct
- TODO - calc nmi
- TODO - collect nmi


nmi - from https://github.com/aaronmcdaid/Overlapping-NMI

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


----------------------------------------
Validation plan
----------------------------------------
0. check that individual id in vcf match to indlist
    compare the ".012.indv" files per class, make sure they are the same, and the same as the indlist
1. vcftools - use --singletons and see if any of the indexes is included in any other class (which is not the doubletons)
1. split_vcfs
    select an individual and a maf, and validate the 012 output:
     - make sure the index exist
     - make sure the individuals data is in the same index


-------------------------------
SANITYCHECK: (/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/)
---------------------------------
    - collect from each class 500 distances files (100 is already done)
    - sum per class 0-499 windows (1_per_class_sum_n_windows)
    - validate windows of 0-499 per class:
        ls /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/*499_*count_dis* | wc -l
        (should have 66 = 17+49)
    - sum all (3_sum_distances_from_all_classes)
    - (cluster) run NetStruct on sums per class (submit_2_netstruct_per_class)
    - run NetStruct on sum of all
        cat /vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499.std*
    - visualize all
    -compare per class to all using nmi (4_run_nmi)
    - collect NMI results to figure - notebooks/collect nmi
---------------------------------

---------------------------------
NOTE regarding num of mac 2 windows
---------------------------------
we have 73511 (although in num_window_per_class we have 73031!!)
How can this be? This is because when we wnated to split the work in mac 2 - we first created 1000 slices, each with some indexes from all classes.
---------------------------------
