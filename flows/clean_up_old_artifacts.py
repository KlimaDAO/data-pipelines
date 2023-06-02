""" Clean up old artifacts flow """
from prefect import flow
from prefect.logging import get_run_logger
import utils
import pendulum

MAX_RETENTION_DAYS = 7


@utils.with_result_storage
@flow()
def clean_up_old_artifacts_flow(result_storage=None):
    """Deletes old artifacts (exept those suffixed with latest)"""
    s3 = utils.get_s3()
    logger = get_run_logger()
    for f in s3.ls(utils.get_s3_path("lake"), detail=True):
        days = pendulum.now().diff(f.get("LastModified")).in_days()
        key = f.get("Key")
        logger.info(f"{key} is {days} days old")
        if days >= MAX_RETENTION_DAYS and not key.endswith("-latest"):
            if not utils.get_param("DRY_RUN"):
                s3.rm_file(key)
            logger.info(" => deleted")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    clean_up_old_artifacts_flow()
