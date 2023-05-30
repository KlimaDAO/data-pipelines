""" Raw Retired pool data flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_retired_pool_data"


@task()
def fetch_retired_pool_data_task():
    """Fetches Retired pool data"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.retires(first=utils.get_max_records())

    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.retiree,
            carbon_offsets.offset.tokenAddress,
            carbon_offsets.offset.bridge,
            carbon_offsets.offset.region,
            carbon_offsets.offset.vintage,
            carbon_offsets.offset.projectID,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.methodology,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.country,
            carbon_offsets.offset.category,
            carbon_offsets.offset.name,
            carbon_offsets.offset.totalRetired,
            carbon_offsets.transaction.id,
            carbon_offsets.transaction._select("from"),
        ]
    )


@task()
def validate_retired_pool_data_task(df):
    """Validates Retired pool data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_retired_pool_data():
    """Fetches Retired pool data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_retired_pool_data_task,
        validate_data_task=validate_retired_pool_data_task,
    )


@flow()
def raw_retired_pool_data_flow(result_storage):
    """Fetches Retired pool data and stores it"""
    raw_retired_pool_data.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_retired_pool_data()
