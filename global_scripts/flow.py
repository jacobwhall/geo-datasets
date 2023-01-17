import os
from pathlib import Path
# from configparser import ConfigParser

from prefect import Flow
# from prefect.filesystems import GitHub

from run_config import RunConfig

# config_file = "malaria_atlas_project/config.ini"
# config = ConfigParser()
# config.read(config_file)

# block_name = config["deploy"]["storage_block"]
# GitHub.load(block_name).get_directory('global_scripts')

# from main import MalariaAtlasProject

# tmp_dir = Path(os.getcwd())


def start_run(dataset_class, run_config: RunConfig):#, dataset_config: dict):
    class_instance = dataset_class()#**dataset_config.dict())
    dataset_class.run(**run_config.dict())


default_run_config = RunConfig(backend="prefect", task_runner="hpc", max_workers=6)

class DatasetFlow(Flow):
    def __init__(self, func, run_config: RunConfig=default_run_config):#, dataset_config: dict=None):
        super().__init__(func, task_runner=run_config.task_runner)

this_flow = DatasetFlow(start_run, default_run_config)
