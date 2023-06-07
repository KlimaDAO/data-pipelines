""" Raw Polygon pools redeemed offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants

DEPENDENCIES = []

SLUG = "raw_polygon_pools_redeemed_offsets"

RENAME_MAP = {
    "redeems_value": "Quantity",
    "redeems_timestamp": "Date",
    "redeems_pool": "Pool",
    "redeems_offset_region": "Region",
}


@task()
def fetch_raw_polygon_pools_redeemed_offsets_task():
    """Fetches Polygon pools redeemed offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.redeems(first=utils.get_max_records())

    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.pool,
        ]
    ).rename(columns=RENAME_MAP)


@task()
def validate_raw_polygon_pools_redeemed_offsets_task(df):
    """Validates Polygon pools redeemed offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_pools_redeemed_offsets_flow(result_storage=None):
    """Fetches Polygon pools redeemed offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_pools_redeemed_offsets_task,
        validate_data_task=validate_raw_polygon_pools_redeemed_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_redeemed_offsets_flow()
