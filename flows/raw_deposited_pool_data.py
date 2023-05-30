""" Raw Deposited pool data flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_deposited_pool_data"


@task()
def fetch_deposited_pool_data_task():
    """Fetches Deposited pool data"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.deposits(first=utils.get_max_records())

    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.pool,
        ]
    )


@task()
def validate_deposited_pool_data_task(df):
    """Validates Deposited pool data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_deposited_pool_data():
    """Fetches Deposited pool data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_deposited_pool_data_task,
        validate_data_task=validate_deposited_pool_data_task,
    )


@flow()
def raw_deposited_pool_data_flow(result_storage):
    """Fetches Deposited pool data and stores it"""
    raw_deposited_pool_data.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_deposited_pool_data()
