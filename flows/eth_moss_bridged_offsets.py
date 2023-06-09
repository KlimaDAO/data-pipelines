""" Raw Ethereum Moss bridged offsets flow """
from prefect import task
import utils


DEPENDENCIES = ["raw_eth_moss_bridged_offsets", "raw_verra_data"]

SLUG = "eth_moss_bridged_offsets"


@task()
def fetch_eth_moss_bridged_offsets_task():
    """Merge raw Ethereum Moss bridged offsets with verra data"""
    df_tx = utils.get_latest_dataframe("raw_eth_bridged_offsets_transactions")
    df = utils.merge_verra(
        "raw_eth_moss_bridged_offsets",
        ["Vintage Start"],
        ["Vintage"]
    )
    # Compute Vintage
    df["Vintage"] = (
        df["Serial Number"].astype(str).str[-15:-11].astype(int)
    )
    # Ajust MCO bridges
    df_tx = df_tx[["Date", "Tx Address"]]
    df = df.merge(
        df_tx,
        how="left",
        left_on="Original Tx Address",
        right_on="Tx Address",
        suffixes=("", "_new"),
    ).reset_index(drop=True)
    df.loc[
        df["Original Tx Address"]
        != "0x0000000000000000000000000000000000000000000000000000000000000000",
        "Date",
    ] = df.loc[
        df["Original Tx Address"]
        != "0x0000000000000000000000000000000000000000000000000000000000000000",
        "Date_new",
    ]
    df = df.drop(columns=["Tx Address", "Date_new"])
    return df


@task()
def validate_eth_moss_bridged_offsets_task(df):
    """Validates Ethereum Moss bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def eth_moss_bridged_offsets_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_moss_bridged_offsets_task,
        validate_data_task=validate_eth_moss_bridged_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    eth_moss_bridged_offsets_flow()
