""" Raw Verra data flow """
import os
import utils


def compute_flows_dict():
    """Create a dictionnary of flows to execute
    The dictionnary keys are the module_name
    The dictionnary values are objects containing:
    - flow_func: The flow function to execute
    - dependencies: the flow_dependencies
    - built: A boolean indicating if the flow has been executed

    """

    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    flows_dict = {}
    for file in os.listdir(script_dir):
        file_path = os.path.join(script_dir, file)
        module_name, file_extension = os.path.splitext(file)
        if (
            os.path.isfile(file_path)
            and file_extension == ".py"
            and module_name != "__init__"
            and module_name != "build_all"
        ):
            module = __import__(module_name)
            if hasattr(module, "DEPENDENCIES"):
                flows_dict[module_name] = {
                    "flow_func": getattr(module, f"{module_name}_flow"),
                    "dependencies":  module.DEPENDENCIES,
                    "built": False
                }
    return flows_dict


@utils.flow_with_result_storage
def build_all_flow(result_storage=None):
    """Build all artifacts using dependency information"""
    logger = utils.get_run_logger()
    flows_dict = compute_flows_dict()

    while True:
        pending_flow_names = [
            key for key in flows_dict.keys()
            if not flows_dict[key]["built"]
        ]

        something_done = False
        # Execute pending flows
        for flow_name in pending_flow_names:
            flow = flows_dict[flow_name]

            # If all dependency are built
            if all(dependency not in pending_flow_names
                   for dependency in flow["dependencies"]):
                logger.info(f"Executing flow {flow_name}")
                flow["flow_func"]()

                # And mark this flow as built
                flows_dict[flow_name]["built"] = True
                something_done = True

        if not something_done:
            break

    if pending_flow_names:
        raise Exception(f"Some artifacts where not built {pending_flow_names}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_all_flow()
