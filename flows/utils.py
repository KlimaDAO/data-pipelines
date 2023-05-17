from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem


def write_file(storage, filename, data):
    """Write a file given bytes

    Arguments:
    block_name: name of a Prefect block or "local" to write to the /tmp directory of the local filesystem
    filename: name of the destination file
    data: data to be written (bytes)
    """
    if storage == "local":
        block = LocalFileSystem(basepath="/tmp")
    else:
        block = Block.load(storage)
    block.write_path(filename, data)
