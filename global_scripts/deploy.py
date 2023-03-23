"""
This script is intended to build and deploy a Prefect flow using settings/parameters defined in an
accompany config.ini for the dataset.

---------------------------------------

Roughly equivalent actions via cli
#
# *** the below commands are just a general starting point, and not meant to run as is. note that there
#     are no parameters specified or storage block creation

# to deploy:
prefect deployment build flow.py:flow_function_name -n "deployment_name" -sb github/existing_storage_block_name -q work_queue_name --apply

# to not immediately deploy remove `--apply` from the above line, then use the build yaml to run the following:
# prefect deployment apply build-deployment.yaml

# to run the deployment
prefect deployment run flow_function_name/deployment_name

# start workqueue
prefect agent start -q 'work_queue_name'

"""

import os
import sys
from pathlib import Path
from configparser import ConfigParser

import click
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from prefect.infrastructure.kubernetes import KubernetesJob


@click.command()
@click.argument("dataset")
@click.option("--kubernetes-job-block", default=None, help="Name of Kubernetes Job block to use")
def deploy(dataset, kubernetes_job_block):
    # find dataset directory
    dataset_dir = Path(dataset)
    if dataset_dir.is_dir():
        click.echo(f"👍 Found dataset {dataset}")
    else:
        raise Exception("dataset directory provided not found in current directory")

    # find and import the get_config_dict function for the dataset
    click.echo("🔎 Finding get_config_dict function for dataset...")
    sys.path.insert(1, dataset_dir.as_posix())
    from main import get_config_dict

    # find and parse dataset config file
    click.echo("⚙️ Finding config.ini file for dataset...")
    config_file = dataset_dir / "config.ini"
    config = ConfigParser()
    config.read(config_file)

    # load flow
    module_name = config["deploy"]["flow_file_name"]
    flow_name = config["deploy"]["flow_name"]

    click.echo("🐙 Creating GitHub storage block...")
    # create and load storage block
    block_name = config["deploy"]["storage_block"]
    block_repo = config["github"]["repo"]
    block_reference = config["github"]["branch"] # branch or tag
    block_repo_dir = config["github"]["directory"]

    block = GitHub(
        repository=block_repo,
        reference=block_reference,
        #access_token=<my_access_token> # only required for private repos
    )
    # block.get_directory(block_repo_dir)
    block.save(block_name, overwrite=True)

    click.echo("⬇️ Loading up flow and storage block...")
    # TODO: prevent flow.py from overwriting this file during import
    module = __import__(module_name)
    flow = getattr(module, flow_name)

    # load a pre-defined block and specify a subfolder of repo
    storage = GitHub.load(block_name)#.get_directory(block_repo_dir)

    customizations = []
    emoji_ord = 128179

    for request_type in ("limit", "request"):
        for resource in ("cpu", "memory"): 
            config_key = f"{resource}_{request_type}"
            if config.has_option("run", config_key):
                amount = str(config["run"][config_key])
                emoji_ord += 1
                click.echo(f"{chr(emoji_ord)} Adding resource {request_type}: {amount} for {resource}")
                if resource == "memory":
                    amount += "Gi"
                
                customizations.append({
                    "op": "replace",
                    "path": f"/spec/template/spec/containers/0/resources/{request_type}s/{resource}",
                    "value": amount,
                })

    if config.has_option("deploy", "data_manager_version"):
        customizations.append({
            "op": "add",
            "path": "/spec/template/spec/containers/0/env/-",
            "value": {
                "name": "DATA_MANAGER_VERSION",
                "value": config["deploy"]["data_manager_version"],
            },
        })
    else:
        raise ValueError("config.ini must include a data manager version")
                    

    deployment_options = {
        "flow": flow,
        "name": config["deploy"]["deployment_name"],
        "version": config["deploy"]["version"],
        # "work_queue_name": "geo-datasets",
        "work_queue_name": config["deploy"]["work_queue"],
        "storage": storage,
        "path": block_repo_dir,
        # "skip_upload": True,
        "parameters": get_config_dict(config_file),
        "apply": True,
    }

    # find Kubernetes Job Block, if one was specified
    if kubernetes_job_block is None:
        click.echo("⛰️ No Kubernetes Job Block will be used.")
    else:
        click.echo(f"🚢 Using Kubernetes Job Block: {kubernetes_job_block}")
        infra_block = KubernetesJob.load(kubernetes_job_block)
        deployment_options["infrastructure"] = infra_block
        if len(customizations) > 0:
            infra_block.customizations.patch.extend(customizations)
            deployment_options["infra_overrides"] = { "customizations": infra_block.customizations.patch }

    # build deployment
    deployment = Deployment.build_from_flow(**deployment_options)

    click.echo("✨ Done!")


if __name__ == "__main__":
    deploy()
