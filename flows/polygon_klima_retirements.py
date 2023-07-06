""" Raw Polygon Klima retirements flow """
import pandas as pd
from prefect import task
import utils


SLUG = "polygon_klima_retirements"
RENAME_MAP = {
    "klimaRetires_transaction_id": "Transaction ID",
    "klimaRetires_beneficiaryAddress": "Beneficiary Address",
    "klimaRetires_offset_projectID": "Project ID",
    "klimaRetires_offset_bridge": "Bridge",
    "klimaRetires_token": "Token",
    "klimaRetires_datetime": "Date",
    "klimaRetires_amount": "Amount",
    "klimaRetires_proof": "Proof"
}


@task()
def fetch_polygon_klima_retirements_task():
    """Fetches Polygon Klima retirements"""
    df = utils.get_latest_dataframe("raw_polygon_klima_retirements")
    df = df.rename(columns=RENAME_MAP)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S")
    return df


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
