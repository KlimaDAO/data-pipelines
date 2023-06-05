""" Raw Polygon daily Klima retirements flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


SLUG = "raw_polygon_klima_retirements_daily"


@task()
def fetch_polygon_klima_retirements_daily_task():
    """Fetches Polygon daily Klima retirements"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)

    carbon_data.DailyKlimaRetirement.datetime = SyntheticField(
        utils.format_timestamp,
        SyntheticField.STRING,
        carbon_data.DailyKlimaRetirement.timestamp,
    )

    daily_klima_retirements = carbon_data.Query.dailyKlimaRetirements(
        orderBy=carbon_data.DailyKlimaRetirement.timestamp,
        orderDirection="desc",
        first=utils.get_max_records()
    )

    return sg.query_df(
        [
            daily_klima_retirements.id,
            daily_klima_retirements.timestamp,
            daily_klima_retirements.datetime,
            daily_klima_retirements.amount,
            daily_klima_retirements.token,
        ]
    )


@task()
def validate_polygon_klima_retirements_daily_task(df):
    """Validates Polygon daily Klima retirements"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_klima_retirements_daily_flow(result_storage=None):
    """Fetches Polygon daily Klima retirements and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_klima_retirements_daily_task,
        validate_data_task=validate_polygon_klima_retirements_daily_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_klima_retirements_daily_flow()
