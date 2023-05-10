from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem

# A facility to write files to a prefect block
# use block_name 'local' to write to the local /tmp folder
def write_file(block_name, filename, data):
    if block_name == "local":
        block = LocalFileSystem(basepath="/tmp")
    else:
        block = Block.load(block_name)
    block.write_path(filename, data)