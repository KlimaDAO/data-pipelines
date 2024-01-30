""" Raw Polygon Klima retirements flow """
import pandas as pd
from prefect import task
import utils
import constants


SLUG = "polygon_klima_retirements"


@task()
def fetch_polygon_klima_retirements_task():
    """Fetches Polygon Klima retirements"""
    df = utils.get_latest_dataframe("raw_polygon_klima_retirements")
    df["Retirement Date"] = pd.to_datetime(df["Retirement Date"], format="%Y-%m-%d %H:%M:%S")
    df["Origin"] = "Klima"

    # Convert token addresses to names
    def token_address_to_name(token_address):
        for token in constants.TOKENS:
            if constants.TOKENS[token]["Token Address"] == token_address:
                return token

    df["Token"] = df["Token"].apply(token_address_to_name)

    return utils.auto_rename_columns(df)


@task()
def validate_polygon_klima_retirements_task(df):
    """Validates Polygon Klima retirements"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_klima_retirements_flow(result_storage=None):
    """Fetches Polygon Klima retirements and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_klima_retirements_task,
        validate_data_task=validate_polygon_klima_retirements_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_klima_retirements_flow()
