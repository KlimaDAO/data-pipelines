""" Raw Polygon pools redeemed offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_redeemed_offsets"

RENAME_MAP = {
    "poolRedeems_amount": "Quantity",
    "poolRedeems_timestamp": "Date",
    "poolRedeems_pool_id": "Pool",
    "poolRedeems_offset_region": "Region",
}


@utils.task_with_backoff
def fetch_raw_polygon_pools_redeemed_offsets_task():
    """Fetches Polygon pools redeemed offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    redeems = carbon_data.Query.poolRedeems(first=utils.get_max_records())

    df = sg.query_df(
        [
            redeems.amount,
            redeems.timestamp,
            redeems.pool.id,
        ]
    ).rename(columns=RENAME_MAP)

    utils.convert_tons(df, ["Quantity"])

    return df


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
