""" Fetch artifacts from S3 and save them locally """
import os
from prefect.logging import get_run_logger
import utils


def fetch_s3_artifact(path):
    logger = get_run_logger()
    logger.info(f"fetching {path}")
    s3 = utils.get_s3()
    filename = os.path.basename(path)
    if not utils.get_param("DRY_RUN"):
        with s3.open(path, 'rb') as f:
            df = utils.read_df_from_bytes(f.read())
            utils.store_raw_data_task.with_options(result_storage_key=filename)(df)


@utils.flow_with_result_storage
def fetch_s3_artifacts(result_storage=None):
    """Deletes old artifacts (exept those suffixed with latest)"""
    s3 = utils.get_s3()
    for f in s3.ls(utils.get_s3_path("lake"), detail=True):
        key = f.get("Key")
        if key.endswith("-latest"):
            fetch_s3_artifact(key)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    fetch_s3_artifacts()
