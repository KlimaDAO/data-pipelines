""" Raw Verra data flow """
from prefect import task
import utils


SLUG = "tokens_data_v2"


@task()
def fetch_tokens_data_v2():
    """Builds Tokens data"""
    df = utils.get_latest_dataframe("tokens_data")

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
