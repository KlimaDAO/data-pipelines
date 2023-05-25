from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem
import pandas as pd
import io


def write_df(storage, filename, data):
    """Write a file given bytes

    Arguments:
    storage: name of a Prefect block or "local" to write to the /tmp directory of the local filesystem
    filename: name of the destination file
    df: dataFrame
    """
    content = data.to_json().encode("utf-8")
    if storage == "local":
        block = LocalFileSystem(basepath="/tmp")
    else:
        block = Block.load(storage)
    block.write_path(filename, content)


def read_df(storage, filename):
    """Reads a file from storage

    Arguments:
    storage: name of a Prefect block or "local" to read from the /tmp directory of the local filesystem
    filename: name of the destination file
    """
    if storage == "local":
        block = LocalFileSystem(basepath="/tmp")
    else:
        block = Block.load(storage)
    data = block.read_path(filename)
    return pd.read_json(io.BytesIO(data))
