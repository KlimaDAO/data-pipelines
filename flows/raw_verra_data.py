from prefect import flow

@flow
def raw_verra_data(name="raw_verra_data"):
    print("What is your favorite number?")
    return 42



print(raw_verra_data())