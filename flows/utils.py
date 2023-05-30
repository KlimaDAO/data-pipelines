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

DATEFORMAT = "%Y_%m_%d__%H_%M_%S"
S3_ENDPOINT = "https://nyc3.digitaloceanspaces.com/"


def get_param(param, default=None):
    """ Returns an execution parameter

    Arguments:
    param: name of the parameter
    default: an optional default value
    """
    result = os.getenv(f"DATA_PIPELINES_{param}")
    return int(result) if result else default


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
    """Returns the result storage block the current flow is runnung on"""
    block = FlowRunContext.get().result_factory.storage_block
    return block


def read_df(filename) -> pd.DataFrame:
    """Reads a dataframe from storage

    Arguments:
    filename: name of the destination file
    """
    file_data = get_storage_block().read_path(filename)
    blob = PersistedResultBlob.parse_raw(file_data).data
    return DfSerializer().loads(blob)


def get_s3():
    """Get a s3fs client instance"""
    return s3fs.S3FileSystem(
      anon=False,
      endpoint_url=S3_ENDPOINT
      )


def get_s3_path(path):
    """Get a s3fs path contextualized with the running flow instance"""
    prefix = get_storage_block()._block_document_name
    return f"{prefix}-klimadao-data/{path}"


def validate_against_latest_dataset(slug, df):
    """Validates a dataframe against the latest dataset
    
    Arguments:
    slug: the slug of the data filename
    df: the dataframe to be validated

    Returns: the latest dataset for further specific validation
    """
    latest_df = None
    try:
        latest_df = read_df(f"{slug}-latest")
    except Exception as err:
        print(err)

    if latest_df is not None:
        assert df.shape[0] >= latest_df.shape[0], "New dataset has a lower number of rows"
        assert df.shape[1] == latest_df.shape[1], "New dataset does not have the same number of colums"
    else:
        print("Live dataframe cannot be read. Skipping validation")
    return latest_df