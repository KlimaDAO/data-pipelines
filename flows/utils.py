"""Utility functions from KlimaDAO data-pipelines"""
import io
import os
import s3fs
import base64
from datetime import datetime
from prefect.context import FlowRunContext
import pandas as pd
from prefect.results import PersistedResultBlob
from prefect.serializers import Serializer
from typing_extensions import Literal

DATEFORMAT = "%m_%d_%Y_%H_%M_%S"


def get_param(param):
    """ Returns an execution parameter prefect block

    Arguments:
    param: name of the parameter
    """
    return os.getenv(f"DATA_PIPELINES_{param}")


def now():
    """ Returns the current time serialized """
    return datetime.now().strftime(DATEFORMAT)


class DfSerializer(Serializer):
    """
    Serializes Dataframes using feather.
    """
    type: Literal["pandas_feather"] = "pandas_feather"

    def dumps(self, df: pd.DataFrame) -> bytes:
        bytestream = io.BytesIO()
        df.to_feather(bytestream)
        bytestream.seek(0)
        return base64.encodebytes(bytestream.read())

    def loads(self, blob: bytes) -> pd.DataFrame:
        bytestream = io.BytesIO(base64.decodebytes(blob))
        return pd.read_feather(bytestream)


def get_storage_block():
    return FlowRunContext.get().result_factory.storage_block


def read_df(filename) -> pd.DataFrame:
    """Reads a dataframe from storage

    Arguments:
    filename: name of the destination file
    """
    file_data = get_storage_block().read_path(filename)
    blob = PersistedResultBlob.parse_raw(file_data).data
    return DfSerializer().loads(blob)


def get_s3():
    return s3fs.S3FileSystem(
      anon=False,
      endpoint_url="https://nyc3.digitaloceanspaces.com/"
      )


def get_s3_path(path):
    prefix = get_storage_block()._block_document_name
    return f"{prefix}-klimadao-data/{path}"
