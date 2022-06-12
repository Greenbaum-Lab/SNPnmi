import json

from utils.config import get_sample_sites_file_name


def get_sample_site_list(options, paths_helper):
    sample_sites_path = get_sample_sites_file_name(options.dataset_name)
    with open(paths_helper.data_dir + sample_sites_path, "r") as f:
        sites_list = f.readlines()
    sites_list = [e.replace('\n', '') for e in sites_list]
    return sorted(sites_list)


def get_site2size(paths_helper):
    with open(f"{paths_helper.sfs_dir}summary/site2size.json", "r") as f:
        site2size = json.load(f)
    return site2size
