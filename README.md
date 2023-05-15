# data-pipelines

Data extraction and processing pipelines to power KlimaDAO products

## Repository structure

- agent: Files to build a docker image and make a Kubernetes deployment of a Prefect agent
- flows: Python flow files
- root directory: Deployment yaml files

## How does it work

- Deployments are installed on Prefect Cloud.
- Prefect Cloud Schedules flow-runs following the instructions in the configured deployments
- Agents pull flow-runs from their pool and executes them.
  - Agents download the flow code from github (Block github/flows)
  - Agents execute the python code. They use other blocks to store data (s3-bucket/dev for Development, s3-bucket/prod for production)

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

## Manage deployments

### View deployments

prefect deployments ls

### Create a new deployment

see. https://docs.prefect.io/latest/concepts/deployments/#deployment-build-options for details

run a command like:

```
FLOW_ENV="dev" FLOW_NAME="raw_verra_data"; prefect deployment build \
 -n "$FLOW_ENV"_"$FLOW_NAME" \
 -p $FLOW_ENV-agent-pool \
 -q default \
 -o $FLOW_ENV-deployment-$FLOW_NAME.yaml \
 --param storage=s3/$FLOW_ENV \
 --storage-block=github/flows \
 --cron="0 0 * * *"  flows/$FLOW_NAME.py:$FLOW_NAME
```

- FLOW_ENV is test or prod
- FLOW_NAME is the name of the flow

this will create a yaml deployment file fl=or the given flow that you can customize

### Install a deployment on Prefect Cloud

Edit the deployment file and run

`FLOW_ENV="test" FLOW_NAME="raw_verra_data"; prefect deployment apply $FLOW_ENV-deployment-$FLOW_NAME.yaml`

## Launch an agent

To launch an agent on the workpool dev-agent-pool for instance

`prefect agent start -p dev-agent-pool`
