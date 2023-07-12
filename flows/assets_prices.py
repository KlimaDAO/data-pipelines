""" Raw Polygon pools retired offsets flow """
from prefect import task
import utils


SLUG = "assets_prices"


@task()
def fetch_assets_prices_task():
    """Fetches Polygon pools retired offsets"""
    df = utils.get_latest_dataframe("raw_assets_prices")
    return utils.auto_rename_columns(df)


@task()
def validate_assets_prices_task(df):
    """Validates Polygon pools retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def assets_prices_flow(result_storage=None):
    """Fetches Polygon pools retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_assets_prices_task,
        validate_data_task=validate_assets_prices_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    assets_prices_flow()
