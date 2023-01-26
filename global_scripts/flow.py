import os
import sys
from pathlib import Path
from typing import Union
from inspect import getmembers, isclass, signature

from prefect import Flow, get_run_logger
from prefect.filesystems import GitHub
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner

from run_config import RunConfig
                

class DatasetFlow(Flow):

    def __call__(self, dataset_path, run_config, dataset_config):
        
        if run_config.backend == "prefect":
            if run_config.task_runner == "concurrent":
                self.task_runner = ConcurrentTaskRunner()
            elif run_config.task_runner in ["dask", "hpc"]:
                from prefect_dask import DaskTaskRunner
                # TODO: add dask kwargs here
                self.task_runner = DaskTaskRunner()
            elif run_config.task_runner == "sequential":
                self.task_runner = SequentialTaskRunner()
            else:
                raise ValueError(f"Task runner not recognized: {v}")

        super().__call__(dataset_path, run_config, dataset_config)


default_run_config = RunConfig(backend="prefect", task_runner="hpc", max_workers=6)

@DatasetFlow
def start_run(dataset_path: Union[str, Path],
              storage_block_name: str,
              dataset_config: dict,
              run_config: RunConfig=default_run_config):
    logger = get_run_logger()

    # determine name of dataset directory
    dataset_dir = Path(dataset_path).name

    logger.info(f"Starting run of {dataset_dir}")

    # load dataset directory from GitHub storage block
    logger.info("Loading dataset directory...")
    GitHub.load(storage_block_name).get_directory(dataset_dir)

    # add dataset directory to sys.path
    logger.info("Inserting dataset directory into sys.path...")
    sys.path.insert(1, (Path(__file__).parent.parent / dataset_dir).as_posix())

    # import main module from dataset
    logger.info("Importing dataset main file...")
    import main

    from dataset import Dataset
    is_dataset_class = lambda m: isclass(m[1]) and issubclass(m[1], Dataset) and m[0] != "Dataset"

    # find dataset function in module main
    try:
        dataset_class = next(m[1] for m in getmembers(main) if is_dataset_class(m))
    except StopIteration:
        raise ValueError("No dataset class found in module main")

    # create instance of dataset class
    logger.info("Creating instance of dataset class")
    class_instance = dataset_class(**dataset_config)

    # run dataset class with run config
    logger.info("Running dataset class instance")
    class_instance.run(**run_config.dict())
