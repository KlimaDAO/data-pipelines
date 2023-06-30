""" Raw Ethereum Moss bridged offsets flow """
from prefect import task
import utils


SLUG = "eth_moss_bridged_offsets_v2"


@task()
def fetch_eth_moss_bridged_offsets_v2_task():
    """Merge raw Ethereum Moss bridged offsets with verra data"""
    df_tx = utils.get_latest_dataframe("raw_eth_bridged_offsets_transactions")
    df = utils.merge_verra(
        "raw_eth_moss_bridged_offsets_v2",
        ["Vintage Start"],
        ["Vintage"], v="_v2"
    )
    # Add bridge information
    df["Bridge"] = "Moss"
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
    df = utils.date_manipulations(df, "Bridged Date")

    # Manipulate project ID
    df["Quantity"] = df["Quantity"].astype(int)
    pat = r"VCS-(?P<id>\d+)"
    repl = (
        lambda m: "[VCS-"
        + m.group("id")
        + "](https://registry.verra.org/app/projectDetail/VCS/"
        + m.group("id")
        + ")"
    )
    df["Project ID"] = (
        df["Project ID"].astype(str).str.replace(pat, repl, regex=True)
    )
    return df


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
