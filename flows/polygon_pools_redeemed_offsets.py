""" Raw Polygon pools redeemed offsets flow """
from prefect import task
import utils


SLUG = "polygon_pools_redeemed_offsets"


@task()
def fetch_polygon_pools_redeemed_offsets_task():
    """Fetches Polygon pools redeemed offsets"""
    df = utils.get_latest_dataframe("raw_polygon_pools_redeemed_offsets")
    df = utils.date_manipulations(df, "Redeemed Date")
    return df


@task()
def validate_polygon_pools_redeemed_offsets_task(df):
    """Validates Polygon pools redeemed offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_pools_redeemed_offsets_flow(result_storage=None):
    """Fetches Polygon pools redeemed offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_pools_redeemed_offsets_task,
        validate_data_task=validate_polygon_pools_redeemed_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_pools_redeemed_offsets_flow()
