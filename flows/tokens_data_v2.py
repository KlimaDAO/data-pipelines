""" Raw Verra data flow """
from prefect import task
import pandas as pd
import utils
import constants

SLUG = "tokens_data_v2"


def filter_df_by_pool(df, pool_address):
    """Filter a dataframe on a pool address"""
    df["pool"] = df["pool"].str.lower()
    df = df[(df["pool"] == pool_address)].reset_index()
    return df


def filter_carbon_pool(pool_address, *dfs):
    """Slices a dataframe on multiple pool addresses"""
    filtered = []
    for df in dfs:
        filtered.append(filter_df_by_pool(df, pool_address))

    return filtered


@task()
def fetch_tokens_data_v2():
    """Builds Tokens data"""
    df_deposited = utils.get_latest_dataframe("polygon_pools_deposited_offsets")
    df_redeemed = utils.get_latest_dataframe("polygon_pools_redeemed_offsets")
    df_bridged_mco2 = utils.get_latest_dataframe("eth_moss_bridged_offsets_v2")
    df_retired_mco2 = utils.get_latest_dataframe("eth_retired_offsets_v2")
    prices = utils.get_latest_dataframe("current_assets_prices").iloc[0]

    tokens = constants.TOKENS.copy()
    web3 = utils.get_polygon_web3()

    for key in constants.TOKENS.keys():
        # Compute total supply
        token = tokens[key]
        if key in ["BCT", "NCT", "UBO", "NBO"]:
            deposited, redeemed = filter_carbon_pool(
                token["address"], df_deposited, df_redeemed
            )
            token["Current Supply"] = (
                deposited["quantity"].sum() - redeemed["quantity"].sum()
            )

        elif key == "MCO2":
            token["Current Supply"] = (
                df_bridged_mco2["quantity"].sum() - df_retired_mco2["quantity"].sum()
            )

        # Compute Fee redeem_factors
        token_address = token["address"]

        if key in ["BCT", "NCT"]:
            contract = web3.eth.contract(
                address=web3.to_checksum_address(token_address),
                abi=utils.load_abi("toucanPoolToken.json"),
            )
            feeRedeemDivider = contract.functions.feeRedeemDivider().call()
            feeRedeemFactor = (
                contract.functions.feeRedeemPercentageInBase().call() / feeRedeemDivider
            )

        elif key in ["UBO", "NBO"]:
            contract = web3.eth.contract(
                address=web3.to_checksum_address(token_address),
                abi=utils.load_abi("c3PoolToken.json"),
            )
            feeRedeemFactor = contract.functions.feeRedeem().call() / 10000

        elif key == "MCO2":
            feeRedeemFactor = 0

        token["Fee Redeem Factor"] = feeRedeemFactor

        # Compute price
        price_column = f'{key.lower()}_price'
        price = prices[price_column]
        token["price"] = price

        # Compute Selective cost value
        token["Selective Cost Value"] = price * (1 + feeRedeemFactor)

    df = pd.DataFrame.from_dict(tokens, orient="index").reset_index(names="Name")

    df = df.drop(columns=["address"])
    df = df.rename(columns={
        "id": "chain"
    })

    return utils.auto_rename_columns(df)


@task()
def validate_tokens_data_v2(df):
    """Validates Tokens data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def tokens_data_v2_flow(result_storage=None):
    """Fetches Tokens data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_tokens_data_v2,
        validate_data_task=validate_tokens_data_v2,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    tokens_data_v2_flow()
