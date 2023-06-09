""" Clean up latests artifacts flow """
from prefect.logging import get_run_logger
import utils


@utils.flow_with_result_storage
def clean_up_latests_artifacts_flow(result_storage=None):
    """Deletes old artifacts (exept those suffixed with latest)"""
    s3 = utils.get_s3()
    logger = get_run_logger()
    for f in s3.ls(utils.get_s3_path("lake"), detail=True):
        key = f.get("Key")
        if key.endswith("-latest"):
            if not utils.get_param("DRY_RUN"):
                s3.rm_file(key)
            logger.info(f"{key} => deleted")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    clean_up_latests_artifacts_flow()
