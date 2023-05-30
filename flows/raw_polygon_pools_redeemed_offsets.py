""" Raw Polygon pools redeemed offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_redeemed_offsets"


@task()
def fetch_polygon_pools_redeemed_offsets_task():
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
    )


@task()
def validate_polygon_pools_redeemed_offsets_task(df):
    """Validates Polygon pools redeemed offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_polygon_pools_redeemed_offsets():
    """Fetches Polygon pools redeemed offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_pools_redeemed_offsets_task,
        validate_data_task=validate_polygon_pools_redeemed_offsets_task,
    )


@flow()
def raw_polygon_pools_redeemed_offsets_flow(result_storage):
    """Fetches Polygon pools redeemed offsets and stores it"""
    raw_polygon_pools_redeemed_offsets.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_redeemed_offsets()
