""" Raw bridged pool data flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import pandas as pd
import utils


SLUG = "raw_bridged_pool_data"
CARBON_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/polygon-bridged-carbon"
)


@task()
def fetch_bridged_pool_data_task():
    """Fetches Bridged pool data"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(CARBON_SUBGRAPH_URL)

    carbon_offsets = carbon_data.Query.carbonOffsets(
        orderBy=carbon_data.CarbonOffset.lastUpdate,
        orderDirection="desc",
        first=utils.get_param("MAX_RECORDS", 50000),
    )

    return sg.query_df(
        [
            carbon_offsets.tokenAddress,
            carbon_offsets.bridge,
            carbon_offsets.region,
            carbon_offsets.vintage,
            carbon_offsets.projectID,
            carbon_offsets.standard,
            carbon_offsets.methodology,
            carbon_offsets.country,
            carbon_offsets.category,
            carbon_offsets.name,
            carbon_offsets.balanceBCT,
            carbon_offsets.balanceNCT,
            carbon_offsets.balanceUBO,
            carbon_offsets.balanceNBO,
            carbon_offsets.totalBridged,
            carbon_offsets.bridges.value,
            carbon_offsets.bridges.timestamp,
        ]
    )


@task()
def validate_bridged_pool_data_task(df):
    """Validates Bridged pool data

    Arguments:
    df: the dataframe to be validated
    """
    utils.validate_against_latest_dataset(SLUG, df)


@task(persist_result=True,
      result_storage_key=f"{SLUG}-{{parameters[suffix]}}",
      result_serializer=utils.DfSerializer())
def store_bridged_pool_data_task(df, suffix):
    """Stores bridged pool data

    Arguments:
    df: the dataframe
    suffix: a date or 'live'
    """
    return df


@flow()
def raw_bridged_pool_data():
    """Fetches bridged pool data and stores it"""
    df = fetch_bridged_pool_data_task()
    validate_bridged_pool_data_task(df)
    store_bridged_pool_data_task(df, utils.now())
    store_bridged_pool_data_task(df, "latest")


@flow()
def raw_bridged_pool_data_flow(result_storage):
    """Fetches bridged pool data and stores it"""
    raw_bridged_pool_data.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_bridged_pool_data()
