""" Raw Polygon pools deposited offsets flow """
from prefect import task
import utils


SLUG = "polygon_pools_deposited_offsets"


@task()
def fetch_polygon_pools_deposited_offsets_task():
    """Fetches Polygon pools deposited offsets"""
    df = utils.get_latest_dataframe("raw_polygon_pools_deposited_offsets")
    df = utils.date_manipulations(df, "Deposited Date")
    return utils.auto_rename_columns(df)


@task()
def validate_polygon_pools_deposited_offsets_task(df):
    """Validates Polygon pools deposited offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_pools_deposited_offsets_flow(result_storage=None):
    """Fetches Polygon pools deposited offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_pools_deposited_offsets_task,
        validate_data_task=validate_polygon_pools_deposited_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_pools_deposited_offsets_flow()
