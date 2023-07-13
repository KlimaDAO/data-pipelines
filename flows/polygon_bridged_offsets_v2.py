""" Raw Polygon bridged offsets flow """
from prefect import task
import utils


SLUG = "polygon_bridged_offsets_v2"


@task()
def fetch_polygon_bridged_offsets_v2_task():
    """Merge raw Polygon bridged offsets with verra data"""

    # FIXME: Was orignally merged with df_verra_toucan
    df = utils.merge_verra_v2("raw_polygon_bridged_offsets")
    df = utils.region_manipulations(df)
    df = utils.vintage_manipulations(df)
    df = utils.date_manipulations(df, "bridged_date")
    df = df.drop(columns=["id"])
    return utils.auto_rename_columns(df)


@task()
def validate_polygon_bridged_offsets_v2_task(df):
    """Validates Polygon bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_bridged_offsets_v2_flow(result_storage=None):
    """Fetches Polygon bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_bridged_offsets_v2_task,
        validate_data_task=validate_polygon_bridged_offsets_v2_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_bridged_offsets_v2_flow()
