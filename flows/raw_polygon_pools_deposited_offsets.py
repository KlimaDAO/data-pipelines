""" Raw Polygon pools deposited offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_deposited_offsets"


RENAME_MAP = {
    "poolDeposits_amount": "Quantity",
    "poolDeposits_timestamp": "Date",
    "poolDeposits_pool_id": "Pool",
    "poolDeposits_offset_region": "Region",
}


@utils.task_with_backoff
def fetch_raw_polygon_pools_deposited_offsets_task():
    """Fetches Polygon pools deposited offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    deposits = carbon_data.Query.poolDeposits(first=utils.get_max_records())

    df = sg.query_df(
        [
            deposits.amount,
            deposits.timestamp,
            deposits.pool.id,
        ]
    ).rename(columns=RENAME_MAP)

    utils.convert_tons(df, ["Quantity"])
    return df


@task()
def validate_raw_polygon_pools_deposited_offsets_task(df):
    """Validates Polygon pools deposited offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_pools_deposited_offsets_flow(result_storage=None):
    """Fetches Polygon pools deposited offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_pools_deposited_offsets_task,
        validate_data_task=validate_raw_polygon_pools_deposited_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_deposited_offsets_flow()
