""" Ethereum retired offsets flow """
from prefect import task
import utils


SLUG = "eth_retired_offsets_v2"


@task()
def fetch_eth_retired_offsets_v2_task():
    """Merge raw Ethereum retired offsets with verra data"""
    df = utils.merge_verra("raw_eth_retired_offsets", v="_v2")
    df_tx = utils.get_latest_dataframe("raw_eth_moss_retired_offsets")
    df = df.merge(
        df_tx,
        how="left",
        left_on="Tx ID",
        right_on="Tx ID",
        suffixes=("", "_moss"),
    )
    df["Beneficiary"] = df["Retiree_moss"]
    df = utils.date_manipulations(df, "Retirement Date")

    df = utils.vintage_manipulations(df)
    return df


@task()
def validate_eth_retired_offsets_v2_task(df):
    """Validates Ethereum retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def eth_retired_offsets_v2_flow(result_storage=None):
    """Fetches Ethereum retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_retired_offsets_v2_task,
        validate_data_task=validate_eth_retired_offsets_v2_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    eth_retired_offsets_v2_flow()
