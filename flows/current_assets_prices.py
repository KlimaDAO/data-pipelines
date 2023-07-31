""" Raw Polygon pools retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants
from pycoingecko import CoinGeckoAPI
import pandas as pd


SLUG = "current_assets_prices"


def uni_v2_pool_price(web3, pool_address, decimals, base_price=1):
    """
    Calculate the price of a SushiSwap liquidity pool, using the provided
    pool address, decimals of the first token, and multiplied by
    base_price if provided for computing multiple pool hops.
    """
    uni_v2_abi = utils.load_abi("uni_v2_pool.json")
    pool_contract = web3.eth.contract(address=pool_address, abi=uni_v2_abi)

    reserves = pool_contract.functions.getReserves().call()
    token_price = reserves[0] * base_price * 10**decimals / reserves[1]

    return token_price


def klima_usdc_price(web3):
    return uni_v2_pool_price(web3, constants.KLIMA_USDC_ADDRESS, constants.USDC_DECIMALS - constants.KLIMA_DECIMALS)


@task()
def fetch_current_assets_prices_task():
    """Fetches latest asset prices"""
    cg = CoinGeckoAPI()
    tokens_dict = constants.TOKENS
    current_price_only_token_list = ["UBO", "NBO"]
    df_prices = pd.DataFrame()
    web3 = utils.get_polygon_web3()
    for i in tokens_dict.keys():
        if i not in current_price_only_token_list:
            data = cg.get_coin_market_chart_from_contract_address_by_id(
                id=tokens_dict[i]["id"],
                vs_currency="usd",
                contract_address=tokens_dict[i]["Token Address"],
                days=1,
            )
            df = pd.DataFrame(data["prices"], columns=["Date", f"{i}_Price"])
            df["Date"] = pd.to_datetime(df["Date"], unit="ms")
            df["Date"] = df["Date"].dt.floor("D")
            if df_prices.empty:
                df_prices = df
            else:
                df_prices = df_prices.merge(df, how="outer", on="Date")
            df_prices = df_prices.sort_values(by="Date", ascending=False)
    for i in current_price_only_token_list:
        if i == "UBO":
            klima_price = klima_usdc_price(web3)
            token_price = uni_v2_pool_price(
                web3,
                web3.to_checksum_address(tokens_dict[i]["Pair Address"]),
                constants.KLIMA_DECIMALS - tokens_dict[i]["Decimals"],
            )
            price = klima_price / token_price
        elif i == "NBO":
            klima_price = klima_usdc_price(web3)
            token_price = uni_v2_pool_price(
                web3,
                web3.to_checksum_address(tokens_dict[i]["Pair Address"]),
                constants.KLIMA_DECIMALS,
            )
            price = token_price * klima_price

        df_prices[f"{i}_Price"] = price

    df = utils.fetch_assets_prices(Subgrounds(), 1)
    print(df)
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
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    current_assets_prices_flow()
