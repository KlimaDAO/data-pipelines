""" Raw Verra data flow """
from prefect import task
import utils


SLUG = "verra_retirements"


@task()
def fetch_verra_retirements_task():
    """Builds Verra retirements data"""
    df = utils.get_latest_dataframe("verra_data_v2")
    df = df.query("~Toucan & ~C3 & ~Moss")
    df = df[df["Status"] == "Retired"]
    df = df.reset_index(drop=True)
    return df


@task()
def validate_verra_retirements_task(df):
    """Validates Verra data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def verra_retirements_flow(result_storage):
    """Fetches Verra data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_verra_retirements_task,
        validate_data_task=validate_verra_retirements_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    verra_retirements_flow()
