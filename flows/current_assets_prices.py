""" Raw Polygon pools retired offsets flow """
from prefect import task
import utils
import constants
import pandas as pd
import time
from subgrounds.subgrounds import Subgrounds


SLUG = "current_assets_prices"


def get_pair_price(df, pair_address):
    return df[df["pairs_id"] == pair_address.lower()].iloc[0]["pairs_currentprice"]


@task()
def fetch_current_assets_prices_task():
    """Fetches latest asset prices"""
    sg = Subgrounds()
    tokens_dict = constants.TOKENS
    df = pd.DataFrame()

    # Fetch data
    pairs_sg = sg.load_subgraph(constants.PAIRS_SUBGRAPH_URL)
    pairs = pairs_sg.Query.pairs()
    df = sg.query_df([
        pairs.id,
        pairs.token0.symbol,
        pairs.token1.symbol,
        pairs.currentprice
    ])

    # Compute prices
    prices = {
        "Date": time.time()
    }
    for i in tokens_dict.keys():
        price = get_pair_price(df, tokens_dict[i]["Pair Address"])
        prices[f"{i}_Price"] = price

    df = pd.DataFrame(prices, index=[0])

    # Add date
    df["Date"] = (
        pd.to_datetime(df["Date"], unit="s")
        .dt.tz_localize("UTC")
        .dt.floor("D")
        .dt.date
    )
    return utils.auto_rename_columns(df)


@task()
def validate_current_assets_prices_task(df):
    """Validates latest asset prices"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def current_assets_prices_flow(result_storage=None):
    """Fetches latest asset prices and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_current_assets_prices_task,
        validate_data_task=validate_current_assets_prices_task,
        historize=False
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    current_assets_prices_flow()
