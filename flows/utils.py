"""Utility functions from KlimaDAO data-pipelines"""
import io
import json
import uuid
import os
from datetime import datetime
from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem
from prefect import artifacts
import pandas as pd

DATEFORMAT = "%m_%d_%Y_%H_%M_%S"


def get_param(param):
    """ Returns an execution parameter prefect block

    Arguments:
    param: name of the parameter
    """
    return os.getenv(f"DATA_PIPELINES_{param}")


def now():
    return datetime.now().strftime(DATEFORMAT)


def get_block(storage):
    """ Returns a prefect block

    Arguments:
    storage: name of a Prefect block or "local" to use to the /tmp directory of the local filesystem
    """
    if storage == "local":
        block = LocalFileSystem(basepath="/tmp")
    else:
        block = Block.load(storage)
    return block


def write_df(storage, slug, data):
    """Saves a dataframe to storage

    Arguments:
    storage: name of a Prefect block or "local"
    filename: name of the destination file
    df: dataFrame
    """
    block = get_block(storage)
    filename = f"{slug}.feather"
    date_str = datetime.now().strftime(DATEFORMAT)
    archive_filename = f"{slug}.{date_str}.feather"

    # Compute new file
    file = io.BytesIO()
    data.to_feather(file)

    # Save archive
    file.seek(0)
    block.write_path(archive_filename, file.read())

    # Create archive artifact
    if storage != "local" or get_param("LOCAL_ARTEFACTS"):
        key = f"{storage}-{uuid.uuid4().hex}"
        description = f"{storage}_{archive_filename.lower()}"
        data = {
            "storage": storage,
            "filename": archive_filename,
            "date": date_str
        }
        artifacts.create_markdown_artifact(key=key,
                                           description=description,
                                           markdown=json.dumps(data, indent=1))

    # Save working copy
    file.seek(0)
    block.write_path(filename, file.read())


def read_df(storage, slug):
    """Reads a dataframe from storage

    Arguments:
    storage: name of a Prefect block or "local"
    filename: name of the destination file
    """
    block = get_block(storage)
    filename = f"{slug}.feather"

    data = block.read_path(filename)
    return pd.read_feather(io.BytesIO(data))
