""" Raw offsets holders data flow """
from prefect import task
import utils


SLUG = "offsets_holders_data"


@task()
def fetch_offsets_holders_data_task():
    """Fetches offsets holders data"""
    df = utils.get_latest_dataframe("raw_offsets_holders_data")
    df["Key"] = df["Klimate_Address"] + "-" + df["Token"]
    df = df.sort_values(by=["Key", "Date"], ascending=False)
    df = df.drop_duplicates(subset=["Key"], keep="first")
    df = (
        df.groupby("Klimate_Address")["Quantity"]
        .sum()
        .to_frame()
        .reset_index()
    )
    df["Klimate Name"] = df["Klimate_Address"]
    for index, i in enumerate(df["Klimate_Address"].tolist()):
        if i == "0x7dd4f0b986f032a44f913bf92c9e8b7c17d77ad7":
            df.loc[index, "Klimate Name"] = "KlimaDAO"
        elif i == "0x1e67124681b402064cd0abe8ed1b5c79d2e02f64":
            df.loc[index, "Klimate Name"] = "Olympus DAO"

    return df


@task()
def validate_offsets_holders_data_task(df):
    """Validates offsets holders data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def offsets_holders_data_flow(result_storage=None):
    """Fetches offsets holders data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_offsets_holders_data_task,
        validate_data_task=validate_offsets_holders_data_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    offsets_holders_data_flow()
