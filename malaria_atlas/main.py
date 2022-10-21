# download incidence data for Plasmodium falciparum species of malaria, data available 2000-2020, downloads as single-band LZW-compressed GeoTIFF files at 2.5 arcminute resolution
# download the zip file for all the data, then extract data for years wanted into the main directory
# link: https://malariaatlas.org/malaria-burden-data-download/

import os
import requests
from zipfile import ZipFile
import warnings
from pathlib import Path

import pandas as pd

from utility import get_current_timestamp
from download import  manage_download, copy_files
from run_tasks import run_tasks

# -------------------------------------

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# change var = if want to download a different variant's data
data_zipFile_url = 'https://data.malariaatlas.org/geoserver/Malaria/ows?service=CSW&version=2.0.1&request=DirectDownload&ResourceId=Malaria:202206_Global_Pf_Incidence_Rate'
data_name = "202206_Global_Pf_Incidence_Rate"



# base_dir = Path('/sciclone/aiddata10/REU/geo')
# raw_data_dir = base_dir / "raw/malaria_data/1km_mosaic"
# processed_data_dir = base_dir / "data/malaria_data/1km_mosaic"
# log_dir = base_dir / "data/malaria_data/1km_mosaic/logs"

base_dir = Path('/home/userx/Desktop')
raw_data_dir = base_dir / "malaria_data/1km_mosaic/raw_data"
processed_data_dir = base_dir / "malaria_data/1km_mosaic/data"
log_dir = base_dir / "malaria_data/1km_mosaic/logs"


# change var = set to year range wanted
year_list = range(2000, 2021)

# change var: If want to change mode to serial need to change to False not "serial"
run_parallel = True

# change var: set max_workers to own max_workers
max_workers = 12

# -------------------------------------

raw_data_dir.mkdir(parents=True, exist_ok=True)
processed_data_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

# test connection
test_request = requests.get("https://data.malariaatlas.org", verify=True)
test_request.raise_for_status()


print("Running data download")



zipFileLocalName = os.path.join(raw_data_dir, data_name + ".zip")

# download data zipFile from url to the local output directory
manage_download(data_zipFile_url, zipFileLocalName)

# create zipFile to check if data was properly downloaded
try:
    dataZip = ZipFile(zipFileLocalName)
except:
    print("Could not read downloaded zipfile")
    raise

# validate years for processing
zip_years = sorted([int(i[-8:-4]) for i in dataZip.namelist() if i.endswith('.tif')])
years = [i for i in year_list if i in zip_years]
if len(year_list) != len(years):
    warnings.warn(f'Years specificed for processing which were not in downloaded data {set(year_list).symmetric_difference(set(years))}')


print("Copying data files")

df_list = []
for year in years:
    year_file_name = f'{data_name}_{year}.tif'
    item = {
        "zipFile": zipFileLocalName,
        "zipfile_loc": year_file_name,
        "output_loc": os.path.join(processed_data_dir, year_file_name)
    }
    df_list.append(item)

df = pd.DataFrame(df_list)

# generate list of tasks to iterate over
flist = list(zip(df["zipFile"], df["zipfile_loc"], df["output_loc"]))

# unzip data zipFile and copy the years wanted
results = run_tasks(copy_files, flist, backend=None, run_parallel=run_parallel, add_error_wrapper=True, max_workers=max_workers)



print("Results:")

# join download function results back to df
results_df = pd.DataFrame(results, columns=["status", "message", "output_loc"])

output_df = df.merge(results_df, on="output_loc", how="left")

errors_df = output_df[output_df["status"] != 0]
print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

output_path = log_dir / f"data_download_{timestamp}.csv"
output_df.to_csv(output_path, index=False)