import os
from pathlib import Path
from typing import Union
from inspect import getmembers, isclass, signature

from prefect import Flow
from prefect.filesystems import GitHub

from run_config import RunConfig


class DatasetFlow(Flow):
    def _run(self, *args, **kwargs):
        print("_run was...ran!")
        super()._run(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        print("__call__ was...called!")
        super().__call__(*args, **kwargs)


is_dataset_class = lambda m: isclass(m[1]) and issubclass(m[1]) and m[0] != "Dataset"

default_run_config = RunConfig(backend="prefect", task_runner="hpc", max_workers=6)

@DatasetFlow
def start_run(dataset_path: Union[str, Path],
              run_config: RunConfig=default_run_config,
              dataset_config: dict={}):
    # determine name of dataset directory
    dataset_dir = Path(dataset_path).name

    # load dataset directory from GitHub storage block
    block_name = dataset_config["deploy"]["storage_block"]
    GitHub.load(block_name).get_directory(dataset_dir)

    # add dataset directory to sys.path
    sys.path.insert(1, (Path(__file__).parent / dataset_dir).as_posix)
    print(sys.path)

    # import main module from dataset
    import main

    # find dataset function in module main
    try:
        dataset_class = next(m[1] for m in getmembers(main) if is_dataset_class(m))
    except StopIteration:
        raise ValueError("No dataset class found in module main")

    # create instance of dataset class
    class_instance = dataset_class(**dataset_config)

    # run dataset class with run config
    dataset_class.run(**run_config.dict())
