""" Clean up old artifacts """
from prefect import flow
import utils
import pendulum

MAX_RETENTION_DAYS = 1


@flow()
def clean_up_old_artifacts():
    """Deletes old artifacts"""
    s3 = utils.get_s3()
    for f in s3.ls(utils.get_s3_path("lake"), detail=True):
        days = pendulum.now().diff(f.get("LastModified")).in_days()
        key = f.get("Key")
        print(f"{key} is {days} days old")
        if days >= MAX_RETENTION_DAYS:
            s3.rm_file(key)
            print(" => deleted")


@flow()
def clean_up_old_artifacts_flow(result_storage):
    """Deletes old artifacts"""
    clean_up_old_artifacts.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    clean_up_old_artifacts_flow(result_storage="s3-bucket/dev")
