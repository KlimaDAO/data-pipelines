""" Raw Polygon daily Klima retirements flow """
import pandas as pd
from prefect import task
import utils


SLUG = "polygon_klima_retirements_daily"
RENAME_MAP = {
    "dailyKlimaRetirements_id": "ID",
    "dailyKlimaRetirements_timestamp": "Timestamp",
    "dailyKlimaRetirements_datetime": "Retirement Date",
    "dailyKlimaRetirements_amount": "Quantity",
    "dailyKlimaRetirements_token": "Token"
}


@task()
def fetch_polygon_klima_retirements_daily_task():
    """Fetches Polygon daily Klima retirements"""
    df = utils.get_latest_dataframe("raw_polygon_klima_retirements_daily")
    df = df.rename(columns=RENAME_MAP)
    df["Retirement Date"] = pd.to_datetime(df["Retirement Date"], format="%Y-%m-%d %H:%M:%S")
    return utils.auto_rename_columns(df)


@task()
def validate_polygon_klima_retirements_daily_task(df):
    """Validates Polygon daily Klima retirements"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_klima_retirements_daily_flow(result_storage=None):
    """Fetches Polygon daily Klima retirements and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_klima_retirements_daily_task,
        validate_data_task=validate_polygon_klima_retirements_daily_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_klima_retirements_daily_flow()
