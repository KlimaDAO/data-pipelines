""" Raw Verra data flow """
import os
import utils
from utils import run


@utils.flow_with_result_storage
def build_frequent_flow(result_storage=None):
    """Run flows that need to be run frequently"""
    # set up environement for subflows
    if result_storage:
        os.environ["DATA_PIPELINES_RESULT_STORAGE"] = result_storage

    run("current_assets_prices")
    run("tokens_data_v2")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_frequent_flow()
