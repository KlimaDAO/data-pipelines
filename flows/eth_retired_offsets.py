""" Ethereum retired offsets flow """
from prefect import task
import utils

DEPENDENCIES = ["raw_eth_retired_offsets", "raw_verra_data"]

SLUG = "eth_retired_offsets"


@task()
def fetch_eth_retired_offsets_task():
    """Merge raw Ethereum retired offsets with verra data"""
    return utils.merge_verra("raw_eth_retired_offsets")


@task()
def validate_eth_retired_offsets_task(df):
    """Validates Ethereum retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def eth_retired_offsets_flow(result_storage=None):
    """Fetches Ethereum retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_retired_offsets_task,
        validate_data_task=validate_eth_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    eth_retired_offsets_flow()
