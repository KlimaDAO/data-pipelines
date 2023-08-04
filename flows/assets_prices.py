""" Raw Polygon pools retired offsets flow """
from prefect import task
import utils
import pandas as pd


SLUG = "assets_prices"


@task()
def fetch_assets_prices_task():
    """Fetches asset prices"""

    # Fetch asset prices prices
    df = utils.get_latest_dataframe("raw_assets_prices")
    df = utils.auto_rename_columns(df)
    latest_assets_prices = utils.get_latest_dataframe("current_assets_prices")

    # Replace latest entry
    latest_date = latest_assets_prices.iloc[0]["date"]
    df.drop(df[df["date"] == latest_date].index, inplace=True)
    df = pd.concat([latest_assets_prices, df]).reset_index(drop=True)

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
        validate_data_task=validate_assets_prices_task
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    assets_prices_flow()
