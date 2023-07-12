""" Raw Verra data flow """
from prefect import task
import pandas as pd
import utils
import constants
import os
import json


SLUG = "tokens_data"


def filter_df_by_pool(df, pool_address):
    """Filter a dataframe on a pool address"""
    df["Pool"] = df["Pool"].str.lower()
    df = df[(df["Pool"] == pool_address)].reset_index()

    return df


def filter_carbon_pool(pool_address, *dfs):
    """Slices a dataframe on multiple pool addresses"""
    filtered = []
    for df in dfs:
        filtered.append(filter_df_by_pool(df, pool_address))

    return filtered


def load_abi(filename):
    """Load a single ABI from the `abis` folder under `src`"""
    script_dir = os.path.dirname(__file__)
    abi_dir = os.path.join(script_dir, "..", "abis")

    with open(os.path.join(abi_dir, filename), "r") as f:
        abi = json.loads(f.read())

    return abi


@task()
def fetch_tokens_data():
    """Builds Tokens data"""
    df_deposited = utils.get_latest_dataframe("raw_polygon_pools_deposited_offsets")
    df_redeemed = utils.get_latest_dataframe("raw_polygon_pools_redeemed_offsets")
    df_bridged_mco2 = utils.get_latest_dataframe("eth_moss_bridged_offsets")
    df_retired_mco2 = utils.get_latest_dataframe("eth_retired_offsets")

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
                deposited["Quantity"].sum() - redeemed["Quantity"].sum()
            )

        elif key == "MCO2":
            token["Current Supply"] = (
                df_bridged_mco2["Quantity"].sum() - df_retired_mco2["Quantity"].sum()
            )

        # Compute Fee redeem_factors
        token_address = token["address"]

        if key in ["BCT", "NCT"]:
            contract = web3.eth.contract(
                address=web3.to_checksum_address(token_address),
                abi=load_abi("toucanPoolToken.json"),
            )
            feeRedeemDivider = contract.functions.feeRedeemDivider().call()
            feeRedeemFactor = (
                contract.functions.feeRedeemPercentageInBase().call() / feeRedeemDivider
            )

        elif key in ["UBO", "NBO"]:
            contract = web3.eth.contract(
                address=web3.to_checksum_address(token_address),
                abi=load_abi("c3PoolToken.json"),
            )
            feeRedeemFactor = contract.functions.feeRedeem().call() / 10000

        elif key == "MCO2":
            feeRedeemFactor = 0

        token["Fee Redeem Factor"] = feeRedeemFactor

    df = pd.DataFrame.from_dict(tokens, orient="index").reset_index(names="Name")

    return utils.auto_rename_columns(df)


@task()
def validate_tokens_data(df):
    """Validates Tokens data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def tokens_data_flow(result_storage=None):
    """Fetches Tokens data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_tokens_data,
        validate_data_task=validate_tokens_data,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    tokens_data_flow()
