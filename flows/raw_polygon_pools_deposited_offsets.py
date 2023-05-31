""" Raw Polygon pools deposited offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_deposited_offsets"


@task()
def fetch_polygon_pools_deposited_offsets_task():
    """Fetches Polygon pools deposited offsets"""
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
def validate_polygon_pools_deposited_offsets_task(df):
    """Validates Polygon pools deposited offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.with_result_storage
@flow()
def raw_polygon_pools_deposited_offsets_flow(result_storage):
    """Fetches Polygon pools deposited offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_pools_deposited_offsets_task,
        validate_data_task=validate_polygon_pools_deposited_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_deposited_offsets_flow()
