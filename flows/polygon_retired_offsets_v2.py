""" Raw Polygon retired offsets flow """
from prefect import task
import utils


SLUG = "polygon_retired_offsets_v2"


@task()
def fetch_polygon_retired_offsets_v2_task():
    """Merge raw Polygon retired offsets with verra data"""

    df = utils.merge_verra_v2("raw_polygon_retired_offsets")
    df = utils.region_manipulations(df)
    df = utils.vintage_manipulations(df)
    df = utils.date_manipulations(df, "retirement_date")
    df = utils.pool_quantities_manipulations(df)

    return utils.auto_rename_columns(df)


@task()
def validate_polygon_retired_offsets_v2_task(df):
    """Validates Polygon retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_retired_offsets_v2_flow(result_storage=None):
    """Fetches Polygon retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_retired_offsets_v2_task,
        validate_data_task=validate_polygon_retired_offsets_v2_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_retired_offsets_v2_flow()
