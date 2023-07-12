""" Raw Ethereum Moss bridged offsets flow """
from prefect import task
import utils


SLUG = "eth_moss_bridged_offsets_v2"


@task()
def fetch_eth_moss_bridged_offsets_v2_task():
    """Merge raw Ethereum Moss bridged offsets with verra data"""
    df_tx = utils.get_latest_dataframe("raw_eth_bridged_offsets_transactions")
    df_tx = utils.auto_rename_columns(df_tx)
    df = utils.merge_verra_v2(
        "raw_eth_moss_bridged_offsets",
        ["vintage_start"],
        ["vintage"]
    )
    # Add bridge information
    df["bridge"] = "Moss"
    # Compute Vintage
    df["vintage"] = (
        df["serial_number"].astype(str).str[-15:-11].astype(int)
    )
    # Ajust MCO bridges
    df_tx = df_tx[["date", "tx_address"]]
    df = df.merge(
        df_tx,
        how="left",
        left_on="original_tx_address",
        right_on="tx_address",
        suffixes=("", "_new"),
    ).reset_index(drop=True)
    df.loc[
        df["original_tx_address"]
        != "0x0000000000000000000000000000000000000000000000000000000000000000",
        "Date",
    ] = df.loc[
        df["original_tx_address"]
        != "0x0000000000000000000000000000000000000000000000000000000000000000",
        "date_new",
    ]
    df = df.drop(columns=["tx_address", "date_new"])
    df = utils.date_manipulations(df, "bridged_date")

    # Manipulate project ID
    df["quantity"] = df["quantity"].astype(int)
    pat = r"VCS-(?P<id>\d+)"
    repl = (
        lambda m: "[VCS-"
        + m.group("id")
        + "](https://registry.verra.org/app/projectDetail/VCS/"
        + m.group("id")
        + ")"
    )
    df["project_id"] = (
        df["project_id"].astype(str).str.replace(pat, repl, regex=True)
    )
    return utils.auto_rename_columns(df)


@task()
def validate_eth_moss_bridged_offsets_v2_task(df):
    """Validates Ethereum Moss bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def eth_moss_bridged_offsets_v2_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_moss_bridged_offsets_v2_task,
        validate_data_task=validate_eth_moss_bridged_offsets_v2_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    eth_moss_bridged_offsets_v2_flow()
