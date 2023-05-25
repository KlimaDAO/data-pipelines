"""Utility functions from KlimaDAO data-pipelines"""
import io
from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem
import pandas as pd


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


def write_df(storage, filename, data):
    """Saves a dataframe to storage

    Arguments:
    storage: name of a Prefect block or "local"
    filename: name of the destination file
    df: dataFrame
    """
    block = get_block(storage)
    file = io.BytesIO()
    data.to_feather(file)
    file.seek(0)
    block.write_path(filename, file.read())


def read_df(storage, filename):
    """Reads a dataframe from storage

    Arguments:
    storage: name of a Prefect block or "local"
    filename: name of the destination file
    """
    block = get_block(storage)
    data = block.read_path(filename)
    return pd.read_feather(io.BytesIO(data))
