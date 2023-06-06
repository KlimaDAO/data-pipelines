""" Raw Ethereum Moss bridged offsets flow """
from prefect import task
import utils


SLUG = "eth_moss_bridged_offsets"


@task()
def fetch_eth_moss_bridged_offsets_task():
    """Merge raw Ethereum Moss bridged offsets with verra data"""
    return utils.merge_verra(
        "raw_eth_moss_bridged_offsets",
        ["Vintage Start"],
        ["Vintage"]
    )


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
