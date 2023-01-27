import os
import sys
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "pm25/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory("global_scripts")

from main import PM25

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]


@flow
def pm25(raw_dir, output_dir, box_config_path, years, skip_existing_downloads, verify_existing_downloads, overwrite_processing, backend, task_runner, run_parallel, max_workers, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    cluster = "vortex"


    hpc_cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "walltime": "4:00:00",
        "cores": 3,
        "processes": 3,
        "memory": "30GB",
        "interface": "ib0",
        "job_extra_directives": [
            "-j oe",
        ],
        "job_script_prologue": [
            "source /usr/local/anaconda3-2021.05/etc/profile.d/conda.csh",
            "module load anaconda3/2021.05",
            "conda activate geodata38",
            f"cd {tmp_dir}",
        ],
        "log_directory": str(timestamp_log_dir),
    }

    dask_cluster_kwargs = {
        "n_workers": 4,
        "threads_per_worker": 1
    }

    class_instance = PM25(raw_dir=raw_dir, output_dir=output_dir, box_config_path=box_config_path, years=years, skip_existing_downloads=skip_existing_downloads, verify_existing_downloads=verify_existing_downloads, overwrite_processing=overwrite_processing)


    if task_runner != 'hpc':
        os.chdir(tmp_dir)


    if task_runner == 'dask':
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster_kwargs=dask_cluster_kwargs)

    elif task_runner == 'hpc':
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=hpc_cluster_kwargs)

    else:
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
