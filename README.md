# data-pipelines

Data extraction and processing pipelines to power KlimaDAO products

## Setup a development environnement

This project requires python 3.7 or later

Create a python environnement and install dependencies

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
prefect cloud login
```

And follow instructions to setup your API key

## Manage deployments new deployment

### reate a deployment

see. https://docs.prefect.io/latest/concepts/deployments/#deployment-build-options for details

In the **deployemnts directory** run a command like:

`prefect deployment build -n raw_verra_data -p default-agent-pool -q test flows/<flow_name>.py:<flow_name>`

this will create a deployment file named **raw_verra_data-deployment.yaml** that you can customize (see Edit a deployment)

### Edit a deployment

Edit the deployment file and run

`prefect deployment apply <flow_name>.yaml`

## Launch an agent

To launch an agent on the workpool default-agent-pool

`prefect agent start -p default-agent-pool`
