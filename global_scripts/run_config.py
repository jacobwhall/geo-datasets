import logging
from typing import Optional
from pathlib import Path

from pydantic import BaseModel, validator

def validate_retries(retries: int):
    if isinstance(retries, int):
        if retries < 0:
            raise ValueError("Number of task retries must be greater than or equal to zero")
        return retries
    else:
        raise TypeError("retries must be an int greater than or equal to zero")

def validate_retry_delay(retry_delay: int):
    if isinstance(retry_delay, int):
        if retry_delay < 0:
            raise ValueError("Retry delay must be greater than or equal to zero")
        return retry_delay
    else:
        raise TypeError("retry_delay must be an int greater than or equal to zero, representing the number of seconds to wait before retrying a task")

class RunConfig(BaseModel):
    backend: str = "local"
    task_runner: Optional[str]
    run_parallel: Optional[bool]
    max_workers: Optional[int]
    chunksize: Optional[int]
    log_dir: str = "logs"
    logger_level: int = logging.INFO
    retries: int = 3
    retry_delay: int = 5
    dask_kwargs: Optional[dict]

    @validator("backend")
    def validate_backend(cls, v):
        if v not in ("local", "mpi", "prefect"):
            raise ValueError(f"Backend not recognized: {v}")
        return v

    @validator("task_runner")
    def validate_task_runner(cls, v, values):
        if values["backend"] == "prefect":
            from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
            from prefect_dask import DaskTaskRunner
            if v == "concurrent":
                return ConcurrentTaskRunner
            elif v in ["dask", "hpc"]:
                return DaskTaskRunner
            elif v == "sequential":
                return SequentialTaskRunner
            else:
                raise ValueError(f"Task runner not recognized: {v}")
        elif v is not None:
            return None # TODO: raise ValueError in this case?

    @validator("run_parallel")
    def validate_run_parallel(cls, v, values):
        if values["backend"] == "local":
            return v
        else:
            return None

    @validator("max_workers")
    def validate_max_workers(cls, v, values):
        if values["backend"] == "mpi":
            return v
        elif values["backend"] == "prefect" and values["task_runner"] == "hpc":
            return v
        else:
            return None

    @validator("chunksize")
    def validate_chunksize(cls, v, values):
        if v < 1:
            raise ValueError("chunksize must be greater than 1")
        elif values["backend"] == "mpi":
            return v
        if values["backend"] == "local" and values["run_parallel"]:
            return v
        else:
            return None

    @validator("log_dir")
    def make_log_dir_path(cls, v):
        log_dir = Path(v)
        os.makedirs(log_dir, exist_ok=True)
        return log_dir

    @validator("retries")
    def call_validate_retries(cls, v):
        return validate_retries(v)

    @validator("retries")
    def call_validate_retry_delay(cls, v):
        return validate_retry_delay(v)

    @validator("dask_kwargs")
    def validate_dask_kwargs(cls, v, values):
        if values["task_runner"] == "dask":
            if "cluster" in v:
                del v["cluster"]
            if "cluster_kwargs" in v:
                del v["cluster_kwargs"]
        elif values["task_runner"] != "hpc":
            return None
        return v
                
