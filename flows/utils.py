"""Utility functions from KlimaDAO data-pipelines"""
import io
import os
import s3fs
import base64
from datetime import datetime
from prefect.context import FlowRunContext
from prefect.logging import get_run_logger
from prefect import task, flow
import pandas as pd
from prefect.results import PersistedResultBlob
from prefect.serializers import Serializer
from typing_extensions import Literal

DATEFORMAT = "%Y_%m_%d__%H_%M_%S"
S3_ENDPOINT = "https://nyc3.digitaloceanspaces.com/"


# Parameters utils


def get_param(param, default=None):
    """ Returns an execution parameter

    Arguments:
    param: name of the parameter
    default: an optional default value
    """
    result = os.getenv(f"DATA_PIPELINES_{param}")
    return result if result else default


def get_param_as_int(param, default=None):
    """ Returns an execution parameter as an integer

    Arguments:
    param: name of the parameter
    default: an optional default value
    """
    return int(get_param(param, default))


def get_max_records():
    """ Returns the number of records to return for graph queries"""
    return get_param_as_int("MAX_RECORDS", 50000)


# Date utils


def now():
    """ Returns the current time serialized """
    return datetime.now().strftime(DATEFORMAT)


def format_timestamp(timestamp):
    """ Formats a dataframe timestamp """
    return str(datetime.fromtimestamp(timestamp))


# Data utils


class DfSerializer(Serializer):
    """Serializes Dataframes using feather """
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


# Flows utils


def validate_against_latest_dataframe(slug, df):
    """Validates a dataframe against the latest dataframe

    Arguments:
    slug: the slug of the data filename
    df: the dataframe to be validated

    Returns: the latest dataframe for further specific validation
    """
    latest_df = None
    logger = get_run_logger()
    try:
        latest_df = read_df(f"{slug}-latest")
    except Exception as err:
        logger.info(str(err))

    if latest_df is not None:
        assert df.shape[0] >= latest_df.shape[0], "New dataframe has a lower number of rows"
        assert df.shape[1] == latest_df.shape[1], "New dataframe does not have the same number of colums"
    else:
        logger.info("Latest dataframe cannot be read. Skipping validation")
    return latest_df


@task(persist_result=True,
      result_serializer=DfSerializer())
def store_raw_data_task(df):
    """Stores data wihout modifying its content"""
    return df


def flow_with_result_storage(func, **decorator_kwargs):
    """Decorates a function as a flow with autodetected result_storage
    The result storage can come from be:
     - the result_storage argument (deployment)
     - the DATA_PIPELINES_RESULT_STORAGE environment variable
    """
    @flow(name=f"{func.__name__}_wrapper", **decorator_kwargs)
    def inner(result_storage=None):
        if not result_storage:
            result_storage = get_param("RESULT_STORAGE")
        decorator_kwargs["result_storage"] = result_storage
        flow_func = flow(func, **decorator_kwargs)
        return flow_func(result_storage)

    return inner


def raw_data_flow(slug, fetch_data_task, validate_data_task):
    """ Fetches raw data and store it

    Parameters:
    slug: the slug of the data filename
    fetch_data_task: the task to fetch the data
    validate_data_task: the task to validate the data
    """

    df = fetch_data_task()
    validate_data_task(df)
    store_raw_data_task.with_options(result_storage_key=f"{slug}-{now()}")(df)
    store_raw_data_task.with_options(result_storage_key=f"{slug}-latest")(df)
