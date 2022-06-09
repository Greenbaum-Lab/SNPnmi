

def get_sample_site_list(sample_site_path):
    with open(sample_site_path, "r") as f:
        sites_list = f.readlines()
    sites_list = [e.replace('\n', '') for e in sites_list]
    return sites_list