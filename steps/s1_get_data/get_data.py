## Download files by config.
## Will check that the sized match, and can retry to download if they are not.
## Note that it can take a few hours to download 100GB, so better run in screen.
import urllib.request
import time
import sys
import os
from os.path import dirname, abspath
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper
from utils.config import *
from utils.checkpoint_helper import *

from utils.loader import Loader


def get_ftp_source(ftp_source_host, ftp_source_path):
    return f'ftp://{ftp_source_host}{ftp_source_path}'

def get_files_by_dataset_name(dataset_name):
    ftp_source_host = get_dataset_ftp_source_host(dataset_name)
    ftp_source_path = get_dataset_ftp_source_path(dataset_name)
    ftp_source = get_ftp_source(ftp_source_host, ftp_source_path)
    print('Ftp source is ' + ftp_source)
    files_names = get_dataset_vcf_files_names(dataset_name) + get_dataset_metadata_files_names(dataset_name)
    print('Files are ' + ','.join(files_names))

    paths_helper = get_paths_helper(dataset_name)
    local_data_folder = paths_helper.data_dir

    print('output folder is ' + local_data_folder)
    os.makedirs(local_data_folder, exist_ok=True)

    download_files(ftp_source, files_names, local_data_folder)
    return validate_downloaded_files(ftp_source_host, ftp_source_path, local_data_folder, files_names, retry=True)


def download_files(ftp_source, files_names, local_data_folder, override=False):
    for f in files_names:
        dest = local_data_folder + f
        if not override and os.path.exists(dest):
            print('File exists in ' + dest)
        else:
            with Loader('Downloading ' + f):
                urllib.request.urlretrieve(ftp_source + f, dest)


def validate_downloaded_files_sizes(ftp_source_host, ftp_source_path, requested_files_names, local_data_folder):
    ftp_user = "anonymous"
    ftp_pass = ""
    ftp = ftplib.FTP(ftp_source_host, ftp_user, ftp_pass)
    ftp.cwd(ftp_source_path)
    downloaded_files = []
    non_valid_files = []
    for file_name in ftp.nlst():
        if file_name in requested_files_names:
            downloaded_files.append(file_name)
            try:
                ftp.cwd(file_name)
            except Exception as e:
                ftp.voidcmd("TYPE I")
                file_size = ftp.size(file_name)
                local_file_size = Path(local_data_folder + file_name).stat().st_size
                if local_file_size != file_size:
                    print(f'file size mismatch: {file_name} - remote: {file_size}, local: {local_file_size}')
                    non_valid_files.append(file_name)
    return downloaded_files, non_valid_files


def validate_downloaded_files(ftp_source_host, ftp_source_path, local_data_folder, requested_files_names, retry=False):
    downloaded_files, non_valid_files = validate_downloaded_files_sizes(ftp_source_host, ftp_source_path, requested_files_names, local_data_folder)
    missing_files = [f for f in requested_files_names if not f in downloaded_files]
    no_missing_files = True
    if len(missing_files) > 0:
        print('Some files were not found:' + ','.join(missing_files))
        no_missing_files = False
    else: 
        print('All required files found on local machine')

    all_files_sizes_validated = True
    if len(non_valid_files) > 0:
        print('Some files sizes did not match:' + ','.join(non_valid_files))
        all_files_sizes_validated = False
        #retry
        if retry:
            print('Retry to download files')
            ftp_source = get_ftp_source(ftp_source_host, ftp_source_path)
            download_files(ftp_source, non_valid_files, local_data_folder, override=True)
            # we only retry once!
            all_files_sizes_validated = validate_downloaded_files(ftp_source_host, ftp_source_path, local_data_folder, non_valid_files, retry=False)
    else:
        print('All required files have same size on local machine as in the FTP server')
    return all_files_sizes_validated & no_missing_files

# wrappers for execution
def get_data(options):
    assert validate_dataset_name(options.dataset_name)
    return get_files_by_dataset_name(options.dataset_name)


def main(options):
    # args should be: dataset_name
    s = time.time()
    is_executed, msg = execute_with_checkpoint(get_data, os.path.basename(__file__), options)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed


if __name__ == "__main__":
    main(sys.argv[1:])