""" Raw assets prices flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils


SLUG = "raw_assets_prices"


@task()
def fetch_raw_assets_prices_task():
    """Fetches assets prices"""
    return utils.fetch_assets_prices(Subgrounds(), utils.get_max_records())


@task()
def validate_raw_assets_prices_task(df):
    """Validates assets prices"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_assets_prices_flow(result_storage=None):
    """Fetches assets prices and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_assets_prices_task,
        validate_data_task=validate_raw_assets_prices_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_assets_prices_flow()
